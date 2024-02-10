using System.Diagnostics;

namespace NATS.Client.Core;

public partial class NatsConnection
{
    internal async ValueTask<NatsRequestSub<TReply>> RequestSubAsync<TRequest, TReply>(
        string subject,
        TRequest? data,
        NatsHeaders? headers = default,
        INatsSerialize<TRequest>? requestSerializer = default,
        INatsDeserialize<TReply>? replySerializer = default,
        NatsPubOpts? requestOpts = default,
        NatsSubOpts? replyOpts = default,
        CancellationToken cancellationToken = default)
    {
        var replyTo = NewInbox();
        var currentActivity = Activity.Current;

        replySerializer ??= Opts.SerializerRegistry.GetDeserializer<TReply>();
        var sub = new NatsRequestSub<TReply>(currentActivity, this, SubscriptionManager.InboxSubBuilder, subject: replyTo, queueGroup: default, replyOpts, replySerializer);
        await SubAsync(sub, cancellationToken).ConfigureAwait(false);

        requestSerializer ??= Opts.SerializerRegistry.GetSerializer<TRequest>();
        await PublishAsync(subject, data, headers, replyTo, requestSerializer, cancellationToken: cancellationToken).ConfigureAwait(false);

        return sub;
    }
}
