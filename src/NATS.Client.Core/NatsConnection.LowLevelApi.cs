using System.Buffers;
using System.Diagnostics;
using NATS.Client.Core.Commands;
using NATS.Client.Core.Internal;

namespace NATS.Client.Core;

public partial class NatsConnection
{
    /// <summary>
    /// Publishes and yields immediately unless the command channel is full in which case
    /// waits until there is space in command channel.
    /// </summary>
    internal ValueTask PubPostAsync(string subject, string? replyTo = default, ReadOnlySequence<byte> payload = default, NatsHeaders? headers = default, CancellationToken cancellationToken = default)
    {
        var activity = Telemetry.NatsActivities.StartActivity(Telemetry.Constants.PublishActivityName, ActivityKind.Producer);
        Telemetry.FillPublishActivity(ref activity, ref headers, subject, replyTo);

        headers?.SetReadOnly();

        if (ConnectionState == NatsConnectionState.Open)
        {
            var command = PublishBytesCommand.Create(_pool, subject, replyTo, headers, payload, activity, cancellationToken);
            return EnqueueCommandAsync(command);
        }
        else
        {
            return WithConnectAsync(subject, replyTo, headers, payload, activity, cancellationToken, static (self, s, r, h, p, a, c) =>
            {
                var command = PublishBytesCommand.Create(self._pool, s, r, h, p, a, c);
                return self.EnqueueCommandAsync(command);
            });
        }
    }

    internal ValueTask PubModelPostAsync<T>(string subject, T? data, INatsSerializer serializer, string? replyTo = default, NatsHeaders? headers = default, CancellationToken cancellationToken = default)
    {
        var current = Activity.Current;
        var activity = Telemetry.NatsActivities.StartActivity(Telemetry.Constants.PublishActivityName, ActivityKind.Producer);
        Activity.Current = current;

        Telemetry.FillPublishActivity(ref activity, ref headers, subject, replyTo);

        headers?.SetReadOnly();

        if (ConnectionState == NatsConnectionState.Open)
        {
            var command = PublishCommand<T>.Create(_pool, subject, replyTo, headers, data, serializer, activity, cancellationToken);
            return EnqueueCommandAsync(command);
        }
        else
        {
            return WithConnectAsync(subject, replyTo, headers, data, serializer, activity, cancellationToken, static (self, s, r, h, d, ser, a, c) =>
            {
                var command = PublishCommand<T>.Create(self._pool, s, r, h, d, ser, a, c);
                return self.EnqueueCommandAsync(command);
            });
        }
    }

    internal ValueTask PubAsync(string subject, string? replyTo = default, ReadOnlySequence<byte> payload = default, NatsHeaders? headers = default, CancellationToken cancellationToken = default)
    {
        var activity = Telemetry.NatsActivities.StartActivity(Telemetry.Constants.PublishActivityName, ActivityKind.Producer);
        Telemetry.FillPublishActivity(ref activity, ref headers, subject, replyTo);

        headers?.SetReadOnly();

        if (ConnectionState == NatsConnectionState.Open)
        {
            var command = AsyncPublishBytesCommand.Create(_pool, GetCancellationTimer(cancellationToken), subject, replyTo, headers, payload, activity);
            if (TryEnqueueCommand(command))
            {
                return command.AsValueTask();
            }
            else
            {
                return EnqueueAndAwaitCommandAsync(command);
            }
        }
        else
        {
            return WithConnectAsync(subject, replyTo, headers, payload, activity, cancellationToken, static (self, s, r, h, p, a, token) =>
            {
                var command = AsyncPublishBytesCommand.Create(self._pool, self.GetCancellationTimer(token), s, r, h, p, a);
                return self.EnqueueAndAwaitCommandAsync(command);
            });
        }
    }

    internal ValueTask PubModelAsync<T>(string subject, T? data, INatsSerializer serializer, string? replyTo = default, NatsHeaders? headers = default, CancellationToken cancellationToken = default)
    {
        var activity = Telemetry.NatsActivities.StartActivity(Telemetry.Constants.PublishActivityName, ActivityKind.Producer);
        Telemetry.FillPublishActivity(ref activity, ref headers, subject, replyTo);

        headers?.SetReadOnly();

        if (ConnectionState == NatsConnectionState.Open)
        {
            var command = AsyncPublishCommand<T>.Create(_pool, GetCancellationTimer(cancellationToken), subject, replyTo, headers, data, serializer, activity);
            if (TryEnqueueCommand(command))
            {
                return command.AsValueTask();
            }
            else
            {
                return EnqueueAndAwaitCommandAsync(command);
            }
        }
        else
        {
            return WithConnectAsync(subject, replyTo, headers, data, serializer, activity, cancellationToken, static (self, s, r, h, v, ser, a, token) =>
            {
                var command = AsyncPublishCommand<T>.Create(self._pool, self.GetCancellationTimer(token), s, r, h, v, ser, a);
                return self.EnqueueAndAwaitCommandAsync(command);
            });
        }
    }

    internal ValueTask SubAsync(string subject, NatsSubOpts? opts, NatsSubBase sub, CancellationToken cancellationToken = default)
    {
        // var activity = Telemetry.NatsActivities.CreateActivity(Telemetry.Constants.PublishActivityName, ActivityKind.Producer);
        //Telemetry.FillPublishActivity(ref activity, ref headers, subject, replyTo);

        if (ConnectionState == NatsConnectionState.Open)
        {
            return SubscriptionManager.SubscribeAsync(subject, opts, sub, cancellationToken);
        }
        else
        {
            return WithConnectAsync(subject, opts, sub, cancellationToken, static (self, s, o, b, token) =>
            {
                return self.SubscriptionManager.SubscribeAsync(s, o, b, token);
            });
        }
    }
}
