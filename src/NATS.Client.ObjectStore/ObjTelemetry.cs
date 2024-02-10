using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using NATS.Client.Core;
using CoreConstants = NATS.Client.Core.Telemetry.Constants;
using JSConstants = NATS.Client.JetStream.JSTelemetry.Constants;

namespace NATS.Client.ObjectStore;

public static class ObjTelemetry
{
    internal static ActivitySource Source { get; } = new(name: "NATS.Client.Obj");

    internal static ActivitySource InternalSource { get; } = new(name: "NATS.Client.Obj.Internal");

    internal static ActivitySource InternalPublishSource { get; } = new(name: "NATS.Client.Obj.Internal.Publish");

    internal static ActivitySource InternalReceiveSource { get; } = new(name: "NATS.Client.Obj.Internal.Receive");

    internal class Constants
    {
        public const string System = "object_store.system";
        public const string Operation = "object_store.nats.operation";
        public const string Key = "object_store.nats.key";
        public const string Bucket = "object_store.nats.bucket";
    }

    [SuppressMessage("StyleCop.CSharp.OrderingRules", "SA1201:Elements should appear in the correct order", Justification = "Class is all constant.")]
    internal static Activity? StartObjectOp(INatsConnection? connection, NatsObjConfig storeCfg, string operation, string key)
    {
        var source = Telemetry.IsNats(Activity.Current) ? InternalSource : Source;

        if (!source.HasListeners())
            return null;

        KeyValuePair<string, object?>[] tags;
        if (connection is NatsConnection { ServerInfo: not null } conn)
        {
            var serverPort = conn.ServerInfo.Port.ToString();
            tags = new KeyValuePair<string, object?>[13];
            tags[0] = new KeyValuePair<string, object?>(Constants.System, "nats");
            tags[1] = new KeyValuePair<string, object?>(Constants.Bucket, storeCfg.Bucket);
            tags[2] = new KeyValuePair<string, object?>(Constants.Operation, operation);
            tags[3] = new KeyValuePair<string, object?>(Constants.Key, key);

            tags[4] = new KeyValuePair<string, object?>(CoreConstants.ClientId, conn.ServerInfo.ClientId.ToString());
            tags[5] = new KeyValuePair<string, object?>(CoreConstants.ServerAddress, conn.ServerInfo.Host);
            tags[6] = new KeyValuePair<string, object?>(CoreConstants.ServerPort, serverPort);
            tags[7] = new KeyValuePair<string, object?>(CoreConstants.NetworkProtoName, "nats");
            tags[8] = new KeyValuePair<string, object?>(CoreConstants.NetworkProtoVersion, conn.ServerInfo.ProtocolVersion.ToString());
            tags[9] = new KeyValuePair<string, object?>(CoreConstants.NetworkPeerAddress, conn.ServerInfo.Host);
            tags[10] = new KeyValuePair<string, object?>(CoreConstants.NetworkPeerPort, serverPort);
            tags[11] = new KeyValuePair<string, object?>(CoreConstants.NetworkLocalAddress, conn.ServerInfo.ClientIp);
            tags[12] = new KeyValuePair<string, object?>(CoreConstants.NetworkTransport, conn.SocketIsWebSocket ? "websocket" : "tcp");
        }
        else
        {
            tags = new KeyValuePair<string, object?>[4];
            tags[0] = new KeyValuePair<string, object?>(Constants.System, "nats");
            tags[1] = new KeyValuePair<string, object?>(Constants.Bucket, storeCfg.Bucket);
            tags[2] = new KeyValuePair<string, object?>(Constants.Operation, operation);
            tags[3] = new KeyValuePair<string, object?>(Constants.Key, key);
        }

        return source.StartActivity(
            operation,
            kind: ActivityKind.Producer,
            parentContext: default, // propagate from current activity
            tags: tags);
    }

    [SuppressMessage("StyleCop.CSharp.OrderingRules", "SA1201:Elements should appear in the correct order", Justification = "Class is all constant.")]
    internal static Activity? StartSealOp(INatsConnection? connection, NatsObjConfig storeCfg)
    {
        var source = Telemetry.IsNats(Activity.Current) ? InternalSource : Source;

        if (!source.HasListeners())
            return null;

        const string operation = "seal";

        KeyValuePair<string, object?>[] tags;
        if (connection is NatsConnection { ServerInfo: not null } conn)
        {
            var serverPort = conn.ServerInfo.Port.ToString();
            tags = new KeyValuePair<string, object?>[12];
            tags[0] = new KeyValuePair<string, object?>(Constants.System, "nats");
            tags[1] = new KeyValuePair<string, object?>(Constants.Operation, operation);
            tags[2] = new KeyValuePair<string, object?>(Constants.Bucket, storeCfg.Bucket);

            tags[3] = new KeyValuePair<string, object?>(CoreConstants.ClientId, conn.ServerInfo.ClientId.ToString());
            tags[4] = new KeyValuePair<string, object?>(CoreConstants.ServerAddress, conn.ServerInfo.Host);
            tags[5] = new KeyValuePair<string, object?>(CoreConstants.ServerPort, serverPort);
            tags[6] = new KeyValuePair<string, object?>(CoreConstants.NetworkProtoName, "nats");
            tags[7] = new KeyValuePair<string, object?>(CoreConstants.NetworkProtoVersion, conn.ServerInfo.ProtocolVersion.ToString());
            tags[8] = new KeyValuePair<string, object?>(CoreConstants.NetworkPeerAddress, conn.ServerInfo.Host);
            tags[9] = new KeyValuePair<string, object?>(CoreConstants.NetworkPeerPort, serverPort);
            tags[10] = new KeyValuePair<string, object?>(CoreConstants.NetworkLocalAddress, conn.ServerInfo.ClientIp);
            tags[11] = new KeyValuePair<string, object?>(CoreConstants.NetworkTransport, conn.SocketIsWebSocket ? "websocket" : "tcp");
        }
        else
        {
            tags = new KeyValuePair<string, object?>[3];
            tags[0] = new KeyValuePair<string, object?>(Constants.System, "nats");
            tags[1] = new KeyValuePair<string, object?>(Constants.Operation, operation);
            tags[2] = new KeyValuePair<string, object?>(Constants.Bucket, storeCfg.Bucket);
        }

        return source.StartActivity(
            operation,
            kind: ActivityKind.Producer,
            parentContext: default, // propagate from current activity
            tags: tags);
    }
}
