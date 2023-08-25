using System.Buffers;
using System.Diagnostics;
using NATS.Client.Core.Internal;

namespace NATS.Client.Core.Commands;

internal sealed class PublishCommand<T> : CommandBase<PublishCommand<T>>
{
    private string? _subject;
    private string? _replyTo;
    private NatsHeaders? _headers;
    private T? _value;
    private INatsSerializer? _serializer;
    private Activity? _activity;
    private CancellationToken _cancellationToken;

    private PublishCommand()
    {
    }

    public override bool IsCanceled => _cancellationToken.IsCancellationRequested;

    public static PublishCommand<T> Create(ObjectPool pool, string subject, string? replyTo, NatsHeaders? headers, T? value, INatsSerializer serializer, Activity? activity, CancellationToken cancellationToken)
    {
        if (!TryRent(pool, out var result))
        {
            result = new PublishCommand<T>();
        }

        result._subject = subject;
        result._replyTo = replyTo;
        result._headers = headers;
        result._value = value;
        result._serializer = serializer;
        result._activity = activity;
        result._cancellationToken = cancellationToken;

        return result;
    }

    public override Activity? GetActivity() => _activity;

    public override void Write(ProtocolWriter writer)
    {
        writer.WritePublish(_subject!, _replyTo, _headers, _value, _serializer!);
    }

    protected override void Reset()
    {
        _subject = default;
        _headers = default;
        _value = default;
        _serializer = null;
        _cancellationToken = default;
        _activity?.Dispose();
        _activity = default;
    }
}

internal sealed class PublishBytesCommand : CommandBase<PublishBytesCommand>
{
    private string? _subject;
    private string? _replyTo;
    private NatsHeaders? _headers;
    private ReadOnlySequence<byte> _payload;
    private Activity? _activity;
    private CancellationToken _cancellationToken;

    private PublishBytesCommand()
    {
    }

    public override bool IsCanceled => _cancellationToken.IsCancellationRequested;

    public static PublishBytesCommand Create(ObjectPool pool, string subject, string? replyTo, NatsHeaders? headers, ReadOnlySequence<byte> payload, Activity? activity, CancellationToken cancellationToken)
    {
        if (!TryRent(pool, out var result))
        {
            result = new PublishBytesCommand();
        }

        result._subject = subject;
        result._replyTo = replyTo;
        result._headers = headers;
        result._payload = payload;
        result._activity = activity;
        result._cancellationToken = cancellationToken;

        return result;
    }

    public override Activity? GetActivity() => _activity;

    public override void Write(ProtocolWriter writer)
    {
        writer.WritePublish(_subject!, _replyTo, _headers, _payload);
    }

    protected override void Reset()
    {
        _subject = default;
        _replyTo = default;
        _headers = default;
        _payload = default;
        _cancellationToken = default;
        _activity?.Dispose();
        _activity = default;
    }
}

internal sealed class AsyncPublishCommand<T> : AsyncCommandBase<AsyncPublishCommand<T>>
{
    private string? _subject;
    private string? _replyTo;
    private NatsHeaders? _headers;
    private T? _value;
    private INatsSerializer? _serializer;
    private Activity? _activity;

    private AsyncPublishCommand()
    {
    }

    public static AsyncPublishCommand<T> Create(ObjectPool pool, CancellationTimer timer, string subject, string? replyTo, NatsHeaders? headers, T? value, INatsSerializer serializer, Activity? activity)
    {
        if (!TryRent(pool, out var result))
        {
            result = new AsyncPublishCommand<T>();
        }

        result._subject = subject;
        result._replyTo = replyTo;
        result._headers = headers;
        result._value = value;
        result._serializer = serializer;
        result._activity = activity;
        result.SetCancellationTimer(timer);

        return result;
    }

    public override Activity? GetActivity() => _activity;

    public override void Write(ProtocolWriter writer)
    {
        writer.WritePublish(_subject!, _replyTo, _headers, _value, _serializer!);
    }

    protected override void Reset()
    {
        _subject = default;
        _headers = default;
        _value = default;
        _serializer = null;
        _activity?.Dispose();
        _activity = default;
    }
}

internal sealed class AsyncPublishBytesCommand : AsyncCommandBase<AsyncPublishBytesCommand>
{
    private string? _subject;
    private string? _replyTo;
    private NatsHeaders? _headers;
    private ReadOnlySequence<byte> _payload;
    private Activity? _activity;

    private AsyncPublishBytesCommand()
    {
    }

    public static AsyncPublishBytesCommand Create(ObjectPool pool, CancellationTimer timer, string subject, string? replyTo, NatsHeaders? headers, ReadOnlySequence<byte> payload, Activity? activity)
    {
        if (!TryRent(pool, out var result))
        {
            result = new AsyncPublishBytesCommand();
        }

        result._subject = subject;
        result._replyTo = replyTo;
        result._headers = headers;
        result._payload = payload;
        result._activity = activity;
        result.SetCancellationTimer(timer);

        return result;
    }

    public override Activity? GetActivity() => _activity;

    public override void Write(ProtocolWriter writer)
    {
        writer.WritePublish(_subject!, _replyTo, _headers, _payload);
    }

    protected override void Reset()
    {
        _subject = default;
        _replyTo = default;
        _headers = default;
        _payload = default;
        _activity?.Dispose();
        _activity = default;
    }
}
