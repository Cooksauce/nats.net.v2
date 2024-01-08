using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace NATS.Client.Core;

public partial class NatsConnection
{
    /// <inheritdoc />
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask PublishAsync(string subject, NatsHeaders? headers = default, string? replyTo = default, NatsPubOpts? opts = default, CancellationToken cancellationToken = default) =>
        ConnectionState != NatsConnectionState.Open
            ? ConnectAndPublishAsync(subject, default, headers, replyTo, NatsRawSerializer<byte[]>.Default, cancellationToken)
            : PublishInnerAsync(subject, default, headers, replyTo, NatsRawSerializer<byte[]>.Default, cancellationToken);

    /// <inheritdoc />
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask PublishAsync<T>(string subject, T? data, NatsHeaders? headers = default, string? replyTo = default, INatsSerialize<T>? serializer = default, NatsPubOpts? opts = default, CancellationToken cancellationToken = default)
    {
        serializer ??= Opts.SerializerRegistry.GetSerializer<T>();
        return ConnectionState != NatsConnectionState.Open
            ? ConnectAndPublishAsync(subject, data, headers, replyTo, serializer, cancellationToken)
            : PublishInnerAsync(subject, data, headers, replyTo, serializer, cancellationToken);
    }

    /// <inheritdoc />
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask PublishAsync<T>(in NatsMsg<T> msg, INatsSerialize<T>? serializer = default, NatsPubOpts? opts = default, CancellationToken cancellationToken = default) =>
        PublishAsync(msg.Subject, msg.Data, msg.Headers, msg.ReplyTo, serializer, opts, cancellationToken);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private async ValueTask ConnectAndPublishAsync<T>(string subject, T? data, NatsHeaders? headers, string? replyTo, INatsSerialize<T> serializer, CancellationToken cancellationToken)
    {
        var connect = ConnectAsync();
        if (connect.IsCompletedSuccessfully)
        {
#pragma warning disable VSTHRD103
            connect.GetAwaiter().GetResult();
#pragma warning restore VSTHRD103
        }
        else
        {
            await connect.AsTask().WaitAsync(Opts.CommandTimeout, cancellationToken).ConfigureAwait(false);
        }

        await PublishInnerAsync(subject, data, headers, replyTo, serializer, cancellationToken).ConfigureAwait(false);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private ValueTask PublishInnerAsync<T>(string subject, T? data, NatsHeaders? headers, string? replyTo, INatsSerialize<T> serializer, CancellationToken cancellationToken = default)
    {
        var activity = Telemetry.StartSendActivity(this, subject, replyTo, ref headers);
        headers?.SetReadOnly();

        var pubTask = CommandWriter.PublishAsync(subject, data, headers, replyTo, serializer, cancellationToken);
        return activity is null
            ? pubTask
            : WrapAndFinishActivityAsync(pubTask, activity);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [SuppressMessage("StyleCop.CSharp.OrderingRules", "SA1204:Static elements should appear before instance elements", Justification = "Method is private")]
    private static async ValueTask WrapAndFinishActivityAsync(ValueTask task, Activity activity)
    {
        try
        {
            await task.ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            Telemetry.SetException(activity, ex);
            throw;
        }
        finally
        {
            activity.Dispose();
        }
    }
}
