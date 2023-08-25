using System.Buffers;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;

namespace NATS.Client.Core;

public readonly record struct NatsMsg(
    string Subject,
    string? ReplyTo,
    int Size,
    NatsHeaders? Headers,
    ReadOnlyMemory<byte> Data,
    INatsConnection? Connection,
    Activity? Activity = null)
{
    internal static NatsMsg Build(
        string subject,
        string? replyTo,
        in ReadOnlySequence<byte>? headersBuffer,
        in ReadOnlySequence<byte> payloadBuffer,
        INatsConnection? connection,
        HeaderParser headerParser,
        Activity? activity = null)
    {
        NatsHeaders? headers = null;

        if (headersBuffer != null)
        {
            headers = new NatsHeaders();
            if (!headerParser.ParseHeaders(new SequenceReader<byte>(headersBuffer.Value), headers))
            {
                throw new NatsException("Error parsing headers");
            }

            headers.SetReadOnly();
        }

        var size = subject.Length
                   + replyTo?.Length ?? 0
                   + headersBuffer?.Length ?? 0
                   + payloadBuffer.Length;

        Telemetry.FillReceiveActivity(ref activity, headers, subject, replyTo);
        return new NatsMsg(subject, replyTo, (int)size, headers, payloadBuffer.ToArray(), connection, activity);
    }

    public ValueTask ReplyAsync(ReadOnlySequence<byte> payload = default, in NatsPubOpts? opts = default, CancellationToken cancellationToken = default)
    {
        CheckReplyPreconditions();
        Activity.Current ??= Activity;
        return Connection.PublishAsync(ReplyTo, payload, opts, cancellationToken);
    }

    public ValueTask ReplyAsync(NatsMsg msg, in NatsPubOpts? opts = default, CancellationToken cancellationToken = default)
    {
        CheckReplyPreconditions();
        Activity.Current ??= Activity;
        return Connection.PublishAsync(msg with { Subject = ReplyTo }, opts, cancellationToken);
    }

    [MemberNotNull(nameof(Connection))]
    [MemberNotNull(nameof(ReplyTo))]
    private void CheckReplyPreconditions()
    {
        if (Connection == default)
        {
            throw new NatsException("unable to send reply; message did not originate from a subscription");
        }

        if (string.IsNullOrWhiteSpace(ReplyTo))
        {
            throw new NatsException("unable to send reply; ReplyTo is empty");
        }
    }
}

public readonly record struct NatsMsg<T>(
    string Subject,
    string? ReplyTo,
    int Size,
    NatsHeaders? Headers,
    T? Data,
    INatsConnection? Connection,
    Activity? Activity = null)
{
    internal static NatsMsg<T> Build(
        string subject,
        string? replyTo,
        in ReadOnlySequence<byte>? headersBuffer,
        in ReadOnlySequence<byte> payloadBuffer,
        INatsConnection? connection,
        HeaderParser headerParser,
        INatsSerializer serializer,
        Activity? activity = null)
    {
        // Consider an empty payload as null or default value for value types. This way we are able to
        // receive sentinels as nulls or default values. This might cause an issue with where we are not
        // able to differentiate between an empty sentinel and actual default value of a struct e.g. 0 (zero).
        var data = payloadBuffer.Length > 0
            ? serializer.Deserialize<T>(payloadBuffer)
            : default;

        NatsHeaders? headers = null;

        if (headersBuffer != null)
        {
            headers = new NatsHeaders();
            if (!headerParser.ParseHeaders(new SequenceReader<byte>(headersBuffer.Value), headers))
            {
                throw new NatsException("Error parsing headers");
            }

            headers.SetReadOnly();
        }

        var size = subject.Length
            + replyTo?.Length ?? 0
            + headersBuffer?.Length ?? 0
            + payloadBuffer.Length;

        Telemetry.FillReceiveActivity(ref activity, headers, subject, replyTo);
        return new NatsMsg<T>(subject, replyTo, (int)size, headers, data, connection, activity);
    }

    public ValueTask ReplyAsync<TReply>(TReply data, in NatsPubOpts? opts = default, CancellationToken cancellationToken = default)
    {
        CheckReplyPreconditions();
        Activity.Current ??= Activity;
        return Connection.PublishAsync(ReplyTo, data, opts, cancellationToken);
    }

    public ValueTask ReplyAsync<TReply>(NatsMsg<TReply> msg)
    {
        CheckReplyPreconditions();
        Activity.Current ??= Activity;
        return Connection.PublishAsync(msg with { Subject = ReplyTo });
    }

    public ValueTask ReplyAsync(ReadOnlySequence<byte> payload = default, in NatsPubOpts? opts = default, CancellationToken cancellationToken = default)
    {
        CheckReplyPreconditions();
        Activity.Current ??= Activity;
        return Connection.PublishAsync(ReplyTo, payload: payload, opts, cancellationToken);
    }

    public ValueTask ReplyAsync(NatsMsg msg)
    {
        CheckReplyPreconditions();
        Activity.Current ??= Activity;
        return Connection.PublishAsync(msg with { Subject = ReplyTo });
    }

    [MemberNotNull(nameof(Connection))]
    [MemberNotNull(nameof(ReplyTo))]
    private void CheckReplyPreconditions()
    {
        if (Connection == default)
        {
            throw new NatsException("unable to send reply; message did not originate from a subscription");
        }

        if (string.IsNullOrWhiteSpace(ReplyTo))
        {
            throw new NatsException("unable to send reply; ReplyTo is empty");
        }
    }
}
