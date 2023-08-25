using System.Diagnostics;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using NATS.Client.Core.Commands;

namespace NATS.Client.Core.Internal;

internal sealed class NatsPipeliningWriteProtocolProcessor : IAsyncDisposable
{
    private readonly ISocketConnection _socketConnection;
    private readonly WriterState _state;
    private readonly ObjectPool _pool;
    private readonly ConnectionStatsCounter _counter;
    private readonly FixedArrayBufferWriter _bufferWriter;
    private readonly Channel<ICommand> _channel;
    private readonly NatsOptions _options;
    private readonly Task _writeLoop;
    private readonly Stopwatch _stopwatch = new Stopwatch();
    private readonly CancellationTokenSource _cancellationTokenSource;
    private int _disposed;

    public NatsPipeliningWriteProtocolProcessor(ISocketConnection socketConnection, WriterState state, ObjectPool pool, ConnectionStatsCounter counter)
    {
        _socketConnection = socketConnection;
        _state = state;
        _pool = pool;
        _counter = counter;
        _bufferWriter = state.BufferWriter;
        _channel = state.CommandBuffer;
        _options = state.Options;
        _cancellationTokenSource = new CancellationTokenSource();
        _writeLoop = Task.Run(WriteLoopAsync);
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Increment(ref _disposed) == 1)
        {
            _cancellationTokenSource.Cancel();
            await _writeLoop.ConfigureAwait(false); // wait for drain writer
        }
    }

    private async Task WriteLoopAsync()
    {
        var reader = _channel.Reader;
        var protocolWriter = new ProtocolWriter(_bufferWriter);
        var logger = _options.LoggerFactory.CreateLogger<NatsPipeliningWriteProtocolProcessor>();
        var writerBufferSize = _options.WriterBufferSize;
        var promiseList = new List<IPromise>(100);
        var inflightCmds = new List<Activity>(100);

        var isEnabledTraceLogging = logger.IsEnabled(LogLevel.Trace);

        try
        {
            // at first, send priority lane(initial command).
            {
                var firstCommands = _state.PriorityCommands;
                if (firstCommands.Count != 0)
                {
                    var count = firstCommands.Count;
                    var tempBuffer = new FixedArrayBufferWriter();
                    var tempWriter = new ProtocolWriter(tempBuffer);
                    foreach (var command in firstCommands)
                    {
                        command.Write(tempWriter);

                        if (command.GetActivity() is { } activity)
                            inflightCmds.Add(activity);

                        if (command is IPromise p)
                        {
                            promiseList.Add(p);
                        }

                        command.Return(_pool); // Promise does not Return but set ObjectPool here.
                    }

                    _state.PriorityCommands.Clear();

                    try
                    {
                        var memory = tempBuffer.WrittenMemory;
                        while (memory.Length > 0)
                        {
                            _stopwatch.Restart();
                            var sent = await _socketConnection.SendAsync(memory).ConfigureAwait(false);
                            _stopwatch.Stop();
                            if (isEnabledTraceLogging)
                            {
                                logger.LogTrace("Socket.SendAsync. Size: {0} BatchSize: {1} Elapsed: {2}ms", sent, count, _stopwatch.Elapsed.TotalMilliseconds);
                            }

                            Interlocked.Add(ref _counter.SentBytes, sent);
                            memory = memory.Slice(sent);
                        }
                    }
                    catch (Exception ex)
                    {
                        _socketConnection.SignalDisconnected(ex);

                        foreach (var inflightCmd in inflightCmds)
                        {
                            var activity = inflightCmd;
                            Telemetry.SetException(ref activity, ex);
                        }

                        foreach (var item in promiseList)
                        {
                            // var activity = item.GetActivity();
                            // Telemetry.SetException(ref activity, ex);
                            item.SetException(ex); // signal failed
                        }

                        return; // when socket closed, finish writeloop.
                    }

                    foreach (var inflightCmd in inflightCmds)
                    {
                        var activity = inflightCmd;
                        Telemetry.SetComplete(ref activity);
                    }

                    inflightCmds.Clear();

                    foreach (var item in promiseList)
                    {
                        item.SetResult();
                    }

                    promiseList.Clear();
                }
            }

            // restore promise(command is exist in bufferWriter) when enter from reconnecting.
            inflightCmds.AddRange(_state.InflightCommands);
            promiseList.AddRange(_state.PendingPromises);
            _state.InflightCommands.Clear();
            _state.PendingPromises.Clear();

            // main writer loop
            while ((_bufferWriter.WrittenCount != 0) || (await reader.WaitToReadAsync(_cancellationTokenSource.Token).ConfigureAwait(false)))
            {
                try
                {
                    var count = 0;
                    while (_bufferWriter.WrittenCount < writerBufferSize && reader.TryRead(out var command))
                    {
                        Interlocked.Decrement(ref _counter.PendingMessages);
                        if (command.IsCanceled)
                        {
                            continue;
                        }

                        if (command is IBatchCommand batch)
                        {
                            count += batch.Write(protocolWriter);
                        }
                        else
                        {
                            command.Write(protocolWriter);

                            if (command.GetActivity() is { } activity)
                                inflightCmds.Add(activity);

                            count++;
                        }

                        if (command is IPromise p)
                        {
                            promiseList.Add(p);
                        }

                        command.Return(_pool); // Promise does not Return but set ObjectPool here.
                    }

                    try
                    {
                        // SendAsync(ReadOnlyMemory) is very efficient, internally using AwaitableAsyncSocketEventArgs
                        // should use cancellation token?, currently no, wait for flush complete.
                        var memory = _bufferWriter.WrittenMemory;
                        while (memory.Length != 0)
                        {
                            _stopwatch.Restart();
                            var sent = await _socketConnection.SendAsync(memory).ConfigureAwait(false);
                            _stopwatch.Stop();
                            if (isEnabledTraceLogging)
                            {
                                logger.LogTrace("Socket.SendAsync. Size: {0} BatchSize: {1} Elapsed: {2}ms", sent, count, _stopwatch.Elapsed.TotalMilliseconds);
                            }

                            if (sent == 0)
                            {
                                throw new SocketClosedException(null);
                            }

                            Interlocked.Add(ref _counter.SentBytes, sent);

                            memory = memory.Slice(sent);
                        }

                        Interlocked.Add(ref _counter.SentMessages, count);

                        foreach (var inflightCmd in inflightCmds)
                        {
                            var activity = inflightCmd;
                            Telemetry.SetComplete(ref activity);
                        }

                        inflightCmds.Clear();

                        _bufferWriter.Reset();

                        foreach (var item in promiseList)
                        {
                            item.SetResult();
                        }

                        promiseList.Clear();
                    }
                    catch (Exception ex)
                    {
                        // may receive from socket.SendAsync

                        // when error, command is dequeued and written buffer is still exists in state.BufferWriter
                        // store current pending promises to state.
                        _state.InflightCommands.AddRange(inflightCmds);
                        _state.PendingPromises.AddRange(promiseList);
                        _socketConnection.SignalDisconnected(ex);
                        return; // when socket closed, finish writeloop.
                    }
                }
                catch (Exception ex)
                {
                    if (ex is SocketClosedException)
                    {
                        return;
                    }

                    try
                    {
                        logger.LogError(ex, "Internal error occured on WriteLoop.");
                    }
                    catch
                    {
                    }
                }
            }
        }
        catch (OperationCanceledException)
        {
        }
        finally
        {
            try
            {
                if (_bufferWriter.WrittenMemory.Length != 0)
                {
                    await _socketConnection.SendAsync(_bufferWriter.WrittenMemory).ConfigureAwait(false);
                }
            }
            catch
            {
            }
        }
    }
}
