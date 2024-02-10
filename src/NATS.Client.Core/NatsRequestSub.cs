using System.Buffers;
using System.Diagnostics;
using System.Runtime.ExceptionServices;
using System.Threading.Channels;
using NATS.Client.Core.Internal;

namespace NATS.Client.Core;

/// <summary>
/// Similar to <see cref="NatsSub{T}"/> but for request/reply pattern.
/// </summary>
/// <typeparam name="T"></typeparam>
/// <remarks>
/// Replies with no trace headers are associate with an activity passed on construction.
/// </remarks>
internal sealed class NatsRequestSub<T> : NatsSubBase, INatsSub<T>
{
    private readonly Activity? _activityContext;
    private readonly Channel<NatsMsg<T>> _msgs;

    internal NatsRequestSub(
        Activity? activityContext,
        NatsConnection connection,
        ISubscriptionManager manager,
        string subject,
        string? queueGroup,
        NatsSubOpts? opts,
        INatsDeserialize<T> serializer,
        CancellationToken cancellationToken = default)
        : base(connection, manager, subject, queueGroup, opts, cancellationToken)
    {
        _activityContext = activityContext;
        _msgs = Channel.CreateBounded<NatsMsg<T>>(
            connection.GetChannelOpts(connection.Opts, opts?.ChannelOpts),
            msg => Connection.OnMessageDropped(this, _msgs?.Reader.Count ?? 0, msg));

        Msgs = new ActivityEndingMsgReader<T>(_msgs.Reader);

        Serializer = serializer;
    }

    public ChannelReader<NatsMsg<T>> Msgs { get; }

    private INatsDeserialize<T> Serializer { get; }

    protected override async ValueTask ReceiveInternalAsync(string subject, string? replyTo, ReadOnlySequence<byte>? headersBuffer, ReadOnlySequence<byte> payloadBuffer)
    {
        Activity.Current = _activityContext;

        var size = subject.Length
                   + (replyTo?.Length ?? 0)
                   + (headersBuffer?.Length ?? 0)
                   + payloadBuffer.Length;

        var activity = Telemetry.Receive(
            connection: Connection,
            subscriptionSubject: Subject,
            queueGroup: QueueGroup,
            subject: subject,
            replyTo: replyTo,
            bodySize: payloadBuffer.Length,
            size: size);

        var natsMsg = ParseMsg(
            activity: activity,
            subject: subject,
            replyTo: replyTo,
            headersBuffer,
            payloadBuffer: in payloadBuffer,
            connection: Connection,
            headerParser: Connection.HeaderParser,
            serializer: Serializer);

        await _msgs.Writer.WriteAsync(natsMsg).ConfigureAwait(false);

        DecrementMaxMsgs();
    }

    protected override void TryComplete() => _msgs.Writer.TryComplete();
}
