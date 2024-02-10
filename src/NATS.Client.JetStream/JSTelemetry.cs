using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using NATS.Client.Core;
using CoreConstants = NATS.Client.Core.Telemetry.Constants;

namespace NATS.Client.JetStream;

public static class JSTelemetry
{
    internal static ActivitySource PublishSource { get; } = new(name: "NATS.Client.JS.Publish");

    internal static ActivitySource ReceiveSource { get; } = new(name: "NATS.Client.JS.Receive");

    internal static ActivitySource ManageSource { get; } = new(name: "NATS.Client.JS.Manage");

    internal static ActivitySource InternalPublishSource { get; } = new(name: "NATS.Client.Internal.JS.Publish");

    internal static ActivitySource InternalReceiveSource { get; } = new(name: "NATS.Client.Internal.JS.Receive");

    internal class Constants
    {
        public const string Stream = "messaging.nats.js.stream";
        public const string Consumer = "messaging.nats.js.consumer";
        public const string ConsumerName = $"{Consumer}.name";
        public const string Acked = "messaging.nats.js.acked";
        public const string Operation = "nats.js.operation";
    }

    [SuppressMessage("StyleCop.CSharp.OrderingRules", "SA1201:Elements should appear in the correct order", Justification = "Class is all constant.")]
    internal static Activity? StartJSPublish(INatsConnection? connection, string subject, string? replyTo)
    {
        var source = Telemetry.IsNats(Activity.Current) ? InternalPublishSource : PublishSource;

        if (!source.HasListeners())
            return null;

        KeyValuePair<string, object?>[] tags;
        if (connection is NatsConnection { ServerInfo: not null } conn)
        {
            var serverPort = conn.ServerInfo.Port.ToString();

            // ReSharper disable once UseCollectionExpression - not supported in dotnet 6.0
            tags = new KeyValuePair<string, object?>[]
            {
                new(CoreConstants.SystemKey, CoreConstants.SystemVal),
                new(CoreConstants.OpKey, CoreConstants.OpPub),
                new(CoreConstants.DestName, subject),
                new(CoreConstants.ClientId, conn.ServerInfo.ClientId.ToString()),
                new(CoreConstants.ServerAddress, conn.ServerInfo.Host),
                new(CoreConstants.ServerPort, serverPort),
                new(CoreConstants.NetworkProtoName, "nats"),
                new(CoreConstants.NetworkProtoVersion, conn.ServerInfo.ProtocolVersion.ToString()),
                new(CoreConstants.NetworkPeerAddress, conn.ServerInfo.Host),
                new(CoreConstants.NetworkPeerPort, serverPort),
                new(CoreConstants.NetworkLocalAddress, conn.ServerInfo.ClientIp),
                new(CoreConstants.NetworkTransport, conn.SocketIsWebSocket ? "websocket" : "tcp"),
                new(CoreConstants.ReplyTo, replyTo),
            };
        }
        else
        {
            // ReSharper disable once UseCollectionExpression - not supported in dotnet 6.0
            tags = new KeyValuePair<string, object?>[]
            {
                new(CoreConstants.SystemKey, CoreConstants.SystemVal),
                new(CoreConstants.OpKey, CoreConstants.OpPub),
                new(CoreConstants.DestName, subject),
                new(CoreConstants.ReplyTo, replyTo),
            };
        }

        return source.StartActivity(
            $"{subject} publish",
            kind: ActivityKind.Producer,
            parentContext: default, // propagate from current activity
            tags: tags);
    }

    [SuppressMessage("StyleCop.CSharp.OrderingRules", "SA1201:Elements should appear in the correct order", Justification = "Class is all constant.")]
    internal static Activity? JSReceive(
        INatsConnection? connection,
        string? streamSubject,
        string stream,
        string consumerInbox,
        string? queueGroup,
        string subject,
        string? replyTo,
        long bodySize,
        long size)
    {
        var source = Telemetry.IsNats(Activity.Current) ? InternalReceiveSource : ReceiveSource;

        if (!source.HasListeners())
            return null;

        KeyValuePair<string, object?>[] tags;
        if (connection is NatsConnection { ServerInfo: not null } conn)
        {
            var serverPort = conn.ServerInfo.Port.ToString();

            var len = 20;
            if (replyTo is not null)
                len++;

            tags = new KeyValuePair<string, object?>[len];
            tags[0] = new KeyValuePair<string, object?>(CoreConstants.SystemKey, CoreConstants.SystemVal);
            tags[1] = new KeyValuePair<string, object?>(CoreConstants.OpKey, CoreConstants.OpRec);
            tags[2] = new KeyValuePair<string, object?>(CoreConstants.DestTemplate, streamSubject);
            tags[3] = new KeyValuePair<string, object?>(CoreConstants.QueueGroup, queueGroup);
            tags[4] = new KeyValuePair<string, object?>(CoreConstants.Subject, subject);
            tags[5] = new KeyValuePair<string, object?>(CoreConstants.DestName, subject);
            tags[6] = new KeyValuePair<string, object?>(CoreConstants.DestPubName, subject);
            tags[7] = new KeyValuePair<string, object?>(CoreConstants.MsgBodySize, bodySize.ToString());
            tags[8] = new KeyValuePair<string, object?>(CoreConstants.MsgTotalSize, size.ToString());

            tags[9] = new KeyValuePair<string, object?>(CoreConstants.ClientId, conn.ServerInfo.ClientId.ToString());
            tags[10] = new KeyValuePair<string, object?>(CoreConstants.ServerAddress, conn.ServerInfo.Host);
            tags[11] = new KeyValuePair<string, object?>(CoreConstants.ServerPort, serverPort);
            tags[12] = new KeyValuePair<string, object?>(CoreConstants.NetworkProtoName, "nats");
            tags[13] = new KeyValuePair<string, object?>(CoreConstants.NetworkProtoVersion, conn.ServerInfo.ProtocolVersion.ToString());
            tags[14] = new KeyValuePair<string, object?>(CoreConstants.NetworkPeerAddress, conn.ServerInfo.Host);
            tags[15] = new KeyValuePair<string, object?>(CoreConstants.NetworkPeerPort, serverPort);
            tags[16] = new KeyValuePair<string, object?>(CoreConstants.NetworkLocalAddress, conn.ServerInfo.ClientIp);
            tags[17] = new KeyValuePair<string, object?>(CoreConstants.NetworkTransport, conn.SocketIsWebSocket ? "websocket" : "tcp");
            tags[18] = new KeyValuePair<string, object?>(
                CoreConstants.DestIsTemporary,
                consumerInbox.StartsWith(conn.InboxPrefix, StringComparison.Ordinal) ? CoreConstants.True : CoreConstants.False);
            tags[19] = new KeyValuePair<string, object?>(Constants.Stream, stream);

            if (replyTo is not null)
                tags[20] = new KeyValuePair<string, object?>(CoreConstants.ReplyTo, replyTo);
        }
        else
        {
            tags = new KeyValuePair<string, object?>[11];
            tags[0] = new KeyValuePair<string, object?>(CoreConstants.SystemKey, CoreConstants.SystemVal);
            tags[1] = new KeyValuePair<string, object?>(CoreConstants.OpKey, CoreConstants.OpRec);
            tags[2] = new KeyValuePair<string, object?>(CoreConstants.DestTemplate, streamSubject);
            tags[3] = new KeyValuePair<string, object?>(CoreConstants.QueueGroup, queueGroup);
            tags[4] = new KeyValuePair<string, object?>(CoreConstants.Subject, subject);
            tags[5] = new KeyValuePair<string, object?>(CoreConstants.DestName, subject);
            tags[6] = new KeyValuePair<string, object?>(CoreConstants.DestPubName, subject);
            tags[7] = new KeyValuePair<string, object?>(CoreConstants.MsgBodySize, bodySize.ToString());
            tags[8] = new KeyValuePair<string, object?>(CoreConstants.MsgTotalSize, size.ToString());
            tags[9] = new KeyValuePair<string, object?>(Constants.Stream, stream);

            if (replyTo is not null)
                tags[10] = new KeyValuePair<string, object?>(CoreConstants.ReplyTo, replyTo);
        }

        // if (headers is null || !TryParseTraceContext(headers, out var context))
        //     context = default;

        return source.CreateActivity(
            string.IsNullOrEmpty(streamSubject) ? "receive" : $"{streamSubject} receive",
            kind: ActivityKind.Consumer,
            parentContext: default,
            tags: tags);

        // if (activity is null)
        // {
        //     headers ??= new NatsHeaders();
        //     headers.SetReceiveActivity(activity);
        // }

        // Debug.Assert(activity is not null, "Activity should not be null b/c we check listeners manually.");
    }

    [SuppressMessage("StyleCop.CSharp.OrderingRules", "SA1201:Elements should appear in the correct order", Justification = "Class is all constant.")]
    internal static Activity? JSDeliver(
        INatsConnection? connection,
        string? streamSubject,
        string stream,
        string consumerInbox,
        string? queueGroup,
        string subject,
        string? replyTo,
        long bodySize,
        long size)
    {
        var source = Telemetry.IsNats(Activity.Current) ? InternalReceiveSource : ReceiveSource;

        if (!source.HasListeners())
            return null;

        KeyValuePair<string, object?>[] tags;
        if (connection is NatsConnection { ServerInfo: not null } conn)
        {
            var serverPort = conn.ServerInfo.Port.ToString();

            var len = 19;
            if (replyTo is not null)
                len++;

            tags = new KeyValuePair<string, object?>[len];
            tags[0] = new KeyValuePair<string, object?>(CoreConstants.SystemKey, CoreConstants.SystemVal);
            tags[1] = new KeyValuePair<string, object?>(CoreConstants.OpKey, CoreConstants.OpRec);
            tags[2] = new KeyValuePair<string, object?>(CoreConstants.DestTemplate, streamSubject);
            tags[3] = new KeyValuePair<string, object?>(CoreConstants.QueueGroup, queueGroup);
            tags[4] = new KeyValuePair<string, object?>(CoreConstants.Subject, subject);
            tags[5] = new KeyValuePair<string, object?>(CoreConstants.DestName, subject);
            tags[6] = new KeyValuePair<string, object?>(CoreConstants.DestPubName, subject);
            tags[7] = new KeyValuePair<string, object?>(CoreConstants.MsgBodySize, bodySize.ToString());
            tags[8] = new KeyValuePair<string, object?>(CoreConstants.MsgTotalSize, size.ToString());

            tags[9] = new KeyValuePair<string, object?>(CoreConstants.ClientId, conn.ServerInfo.ClientId.ToString());
            tags[10] = new KeyValuePair<string, object?>(CoreConstants.ServerAddress, conn.ServerInfo.Host);
            tags[11] = new KeyValuePair<string, object?>(CoreConstants.ServerPort, serverPort);
            tags[12] = new KeyValuePair<string, object?>(CoreConstants.NetworkProtoName, "nats");
            tags[13] = new KeyValuePair<string, object?>(CoreConstants.NetworkProtoVersion, conn.ServerInfo.ProtocolVersion.ToString());
            tags[14] = new KeyValuePair<string, object?>(CoreConstants.NetworkPeerAddress, conn.ServerInfo.Host);
            tags[15] = new KeyValuePair<string, object?>(CoreConstants.NetworkPeerPort, serverPort);
            tags[16] = new KeyValuePair<string, object?>(CoreConstants.NetworkLocalAddress, conn.ServerInfo.ClientIp);
            tags[17] = new KeyValuePair<string, object?>(CoreConstants.NetworkTransport, conn.SocketIsWebSocket ? "websocket" : "tcp");
            tags[18] = new KeyValuePair<string, object?>(
                CoreConstants.DestIsTemporary,
                consumerInbox.StartsWith(conn.InboxPrefix, StringComparison.Ordinal) ? CoreConstants.True : CoreConstants.False);
            tags[19] = new KeyValuePair<string, object?>(Constants.Stream, stream);

            if (replyTo is not null)
                tags[20] = new KeyValuePair<string, object?>(CoreConstants.ReplyTo, replyTo);
        }
        else
        {
            tags = new KeyValuePair<string, object?>[11];
            tags[0] = new KeyValuePair<string, object?>(CoreConstants.SystemKey, CoreConstants.SystemVal);
            tags[1] = new KeyValuePair<string, object?>(CoreConstants.OpKey, CoreConstants.OpRec);
            tags[2] = new KeyValuePair<string, object?>(CoreConstants.DestTemplate, streamSubject);
            tags[3] = new KeyValuePair<string, object?>(CoreConstants.QueueGroup, queueGroup);
            tags[4] = new KeyValuePair<string, object?>(CoreConstants.Subject, subject);
            tags[5] = new KeyValuePair<string, object?>(CoreConstants.DestName, subject);
            tags[6] = new KeyValuePair<string, object?>(CoreConstants.DestPubName, subject);
            tags[7] = new KeyValuePair<string, object?>(CoreConstants.MsgBodySize, bodySize.ToString());
            tags[8] = new KeyValuePair<string, object?>(CoreConstants.MsgTotalSize, size.ToString());
            tags[9] = new KeyValuePair<string, object?>(Constants.Stream, stream);

            if (replyTo is not null)
                tags[10] = new KeyValuePair<string, object?>(CoreConstants.ReplyTo, replyTo);
        }

        // https://github.com/open-telemetry/semantic-conventions/blob/v1.23.0/docs/messaging/messaging-spans.md#operation-names
        return source.CreateActivity(
            $"{streamSubject} deliver",
            kind: ActivityKind.Consumer,
            parentContext: default,
            tags: tags);
    }

    internal static Activity? StartJSOperation(INatsConnection? connection, string operationName, string? stream, string? consumer = null)
    {
        var source = ManageSource;
        if (!source.HasListeners())
            return null;

        KeyValuePair<string, object?>[] tags;
        if (connection is NatsConnection { ServerInfo: not null } conn)
        {
            var serverPort = conn.ServerInfo.Port.ToString();

            // ReSharper disable once UseCollectionExpression - not supported in dotnet 6.0
            tags = new KeyValuePair<string, object?>[]
            {
                new(CoreConstants.SystemKey, CoreConstants.SystemVal),
                new(CoreConstants.OpKey, operationName),
                // new(CoreConstants.DestName, subject),
                new(CoreConstants.ClientId, conn.ServerInfo.ClientId.ToString()),
                new(CoreConstants.ServerAddress, conn.ServerInfo.Host),
                new(CoreConstants.ServerPort, serverPort),
                new(CoreConstants.NetworkProtoName, "nats"),
                new(CoreConstants.NetworkProtoVersion, conn.ServerInfo.ProtocolVersion.ToString()),
                new(CoreConstants.NetworkPeerAddress, conn.ServerInfo.Host),
                new(CoreConstants.NetworkPeerPort, serverPort),
                new(CoreConstants.NetworkLocalAddress, conn.ServerInfo.ClientIp),
                new(CoreConstants.NetworkTransport, conn.SocketIsWebSocket ? "websocket" : "tcp"),
                // new(CoreConstants.ReplyTo, replyTo),
                new(Constants.Stream, stream),
                new(Constants.ConsumerName, consumer),
                new(Constants.Operation, operationName),
            };
        }
        else
        {
            // ReSharper disable once UseCollectionExpression - not supported in dotnet 6.0
            tags = new KeyValuePair<string, object?>[]
            {
                new(CoreConstants.SystemKey, CoreConstants.SystemVal),
                new(CoreConstants.OpKey, CoreConstants.OpPub),
                // new(CoreConstants.DestName, subject),
                // new(CoreConstants.ReplyTo, replyTo),
                new(Constants.Stream, stream),
                new(Constants.ConsumerName, consumer),
                new(Constants.Operation, operationName),
            };
        }

        return source.StartActivity(
            $"{stream} {operationName}",
            kind: ActivityKind.Internal,
            parentContext: default,
            tags: tags);
    }

    internal static Activity? StartJSControlFlow(INatsConnection? connection, string operationName, string? stream, string? consumer = null)
    {
        var source = ManageSource;
        if (!source.HasListeners())
            return null;

        KeyValuePair<string, object?>[] tags;
        if (connection is NatsConnection { ServerInfo: not null } conn)
        {
            var serverPort = conn.ServerInfo.Port.ToString();

            // ReSharper disable once UseCollectionExpression - not supported in dotnet 6.0
            tags = new KeyValuePair<string, object?>[]
            {
                new(CoreConstants.SystemKey, CoreConstants.SystemVal),
                new(CoreConstants.OpKey, operationName),
                // new(CoreConstants.DestName, subject),
                new(CoreConstants.ClientId, conn.ServerInfo.ClientId.ToString()),
                new(CoreConstants.ServerAddress, conn.ServerInfo.Host),
                new(CoreConstants.ServerPort, serverPort),
                new(CoreConstants.NetworkProtoName, "nats"),
                new(CoreConstants.NetworkProtoVersion, conn.ServerInfo.ProtocolVersion.ToString()),
                new(CoreConstants.NetworkPeerAddress, conn.ServerInfo.Host),
                new(CoreConstants.NetworkPeerPort, serverPort),
                new(CoreConstants.NetworkLocalAddress, conn.ServerInfo.ClientIp),
                new(CoreConstants.NetworkTransport, conn.SocketIsWebSocket ? "websocket" : "tcp"),
                // new(CoreConstants.ReplyTo, replyTo),
                new(Constants.Stream, stream),
                new(Constants.ConsumerName, consumer),
                new(Constants.Operation, operationName),
            };
        }
        else
        {
            // ReSharper disable once UseCollectionExpression - not supported in dotnet 6.0
            tags = new KeyValuePair<string, object?>[]
            {
                new(CoreConstants.SystemKey, CoreConstants.SystemVal),
                new(CoreConstants.OpKey, CoreConstants.OpPub),
                // new(CoreConstants.DestName, subject),
                // new(CoreConstants.ReplyTo, replyTo),
                new(Constants.Stream, stream),
                new(Constants.ConsumerName, consumer),
                new(Constants.Operation, operationName),
            };
        }

        return source.StartActivity(
            $"{stream} {operationName}",
            kind: ActivityKind.Internal,
            parentContext: default,
            tags: tags);
    }
}
