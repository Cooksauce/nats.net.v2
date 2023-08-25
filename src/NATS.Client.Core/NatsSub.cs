using System.Buffers;
using System.Diagnostics;
using System.Runtime.ExceptionServices;
using System.Threading.Channels;
using NATS.Client.Core.Internal;

namespace NATS.Client.Core;

public sealed class NatsSub : NatsSubBase, INatsSub
{
    private static readonly BoundedChannelOptions DefaultChannelOptions =
        new BoundedChannelOptions(1_000)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleWriter = true,
            SingleReader = false,
            AllowSynchronousContinuations = false,
        };

    private readonly Channel<NatsMsg> _msgs;

    internal NatsSub(NatsConnection connection, ISubscriptionManager manager, string subject, NatsSubOpts? opts)
        : base(connection, manager, subject, opts)
    {
        _msgs = Channel.CreateBounded<NatsMsg>(
            GetChannelOptions(opts?.ChannelOptions));

        Msgs = new ActivityChannelReader(_msgs.Reader);
    }

    public ChannelReader<NatsMsg> Msgs { get; }

    internal static BoundedChannelOptions GetChannelOptions(
        NatsSubChannelOpts? subChannelOpts)
    {
        if (subChannelOpts != null)
        {
            var overrideOpts = subChannelOpts.Value;
            return new BoundedChannelOptions(overrideOpts.Capacity ??
                                             DefaultChannelOptions.Capacity)
            {
                AllowSynchronousContinuations =
                    DefaultChannelOptions.AllowSynchronousContinuations,
                FullMode =
                    overrideOpts.FullMode ?? DefaultChannelOptions.FullMode,
                SingleWriter = DefaultChannelOptions.SingleWriter,
                SingleReader = DefaultChannelOptions.SingleReader,
            };
        }
        else
        {
            return DefaultChannelOptions;
        }
    }

    protected override async ValueTask ReceiveInternalAsync(string subject, string? replyTo, ReadOnlySequence<byte>? headersBuffer, ReadOnlySequence<byte> payloadBuffer)
    {
        Activity.Current = null;

        var natsMsg = NatsMsg.Build(
            subject,
            replyTo,
            headersBuffer,
            payloadBuffer,
            Connection,
            Connection.HeaderParser,
            Telemetry.NatsActivities.StartActivity(Telemetry.Constants.ReceiveActivityName, ActivityKind.Consumer));

        await _msgs.Writer.WriteAsync(natsMsg).ConfigureAwait(false);

        DecrementMaxMsgs();
    }

    protected override void TryComplete() => _msgs.Writer.TryComplete();
}

public sealed class NatsSub<T> : NatsSubBase, INatsSub<T>
{
    private readonly Channel<NatsMsg<T?>> _msgs;

    internal NatsSub(
        NatsConnection connection,
        ISubscriptionManager manager,
        string subject,
        NatsSubOpts? opts,
        INatsSerializer serializer)
        : base(connection, manager, subject, opts)
    {
        _msgs = Channel.CreateBounded<NatsMsg<T?>>(
            NatsSub.GetChannelOptions(opts?.ChannelOptions));

        Msgs = new ActivityChannelReader<T?>(_msgs.Reader);

        Serializer = serializer;
    }

    public ChannelReader<NatsMsg<T?>> Msgs { get; }

    private INatsSerializer Serializer { get; }

    protected override async ValueTask ReceiveInternalAsync(string subject, string? replyTo, ReadOnlySequence<byte>? headersBuffer, ReadOnlySequence<byte> payloadBuffer)
    {
        Activity.Current = null;

        var natsMsg = NatsMsg<T?>.Build(
            subject,
            replyTo,
            headersBuffer,
            payloadBuffer,
            Connection,
            Connection.HeaderParser,
            Serializer,
            Telemetry.NatsActivities.StartActivity(Telemetry.Constants.ReceiveActivityName, ActivityKind.Consumer));

        await _msgs.Writer.WriteAsync(natsMsg).ConfigureAwait(false);

        DecrementMaxMsgs();
    }

    protected override void TryComplete() => _msgs.Writer.TryComplete();
}

public class NatsSubException : NatsException
{
    public NatsSubException(string message, ExceptionDispatchInfo exception, Memory<byte> payload, Memory<byte> headers)
        : base(message)
    {
        Exception = exception;
        Payload = payload;
        Headers = headers;
    }

    public ExceptionDispatchInfo Exception { get; }

    public Memory<byte> Payload { get; }

    public Memory<byte> Headers { get; }
}
