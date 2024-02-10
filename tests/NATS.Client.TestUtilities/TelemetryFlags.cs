namespace NATS.Client.TestUtilities;

[Flags]
public enum TelemetryFlags
{
    None = 0,
    CorePublish = 1 << 0,
    CoreReceive = 1 << 1,
    CoreInternalPublish = 1 << 2,
    CoreInternalReceive = 1 << 3,
    JSPublish = 1 << 4,
    JSReceive = 1 << 5,
    JSManage = 1 << 6,
    JSInternalPublish = 1 << 7,
    JSInternalReceive = 1 << 8,
    KvPublish = 1 << 9,
    KvReceive = 1 << 10,
    KvInternalPublish = 1 << 11,
    KvInternalReceive = 1 << 12,
    ObjPublish = 1 << 13,
    ObjReceive = 1 << 14,
    ObjInternalPublish = 1 << 15,
    ObjInternalReceive = 1 << 16,
    All = ~None,
}
