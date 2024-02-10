using System.Diagnostics;
using NATS.Client.TestUtilities;
using static NATS.Client.TestUtilities.TelemetryFlags;
using static NATS.Client.TestUtilities.TelemetryUtils;

namespace NATS.Client.Core.Tests;

public abstract partial class NatsConnectionTest
{
    private static readonly ActivitySource TestActivities = new("Tests.NATS.Client.Core");

    [Fact]
    public async Task PubSubWithTelemetryOffTest()
    {
        using var listener = StartActivityListener(TestActivities.Name);
        await using var server = NatsServer.Start(_output, _transportType);

        await using var nats = server.CreateClientConnection();

        var sync = 0;
        var signal1 = new WaitSignal<NatsMsg<int>>();
        var signal2 = new WaitSignal<NatsMsg<int>>();
        var sub = await nats.SubscribeCoreAsync<int>("foo");
        var reg = sub.Register(m =>
        {
            if (m.Data < 10)
            {
                Interlocked.Exchange(ref sync, m.Data);
                return;
            }

            if (m.Data == 100)
                signal1.Pulse(m);
            if (m.Data == 200)
                signal2.Pulse(m);
        });

        await Retry.Until(
            "subscription is active",
            () => Volatile.Read(ref sync) == 1,
            async () => await nats.PublishAsync("foo", 1));

        var headers = new NatsHeaders { ["Test-Header-Key"] = "test-header-value", ["Multi"] = new[] { "multi-value-0", "multi-value-1" }, };
        Assert.False(headers.IsReadOnly);

        var pubActivity1 = await WithActivity(() => nats.PublishAsync("foo", 100, headers: headers));
        var msg1 = await signal1;

        Assert.NotNull(pubActivity1);
        Assert.Null(msg1.ReceiveActivity);
        Assert.Equal(pubActivity1.Context.TraceId, msg1.GetActivityContext().TraceId);
        Assert.Equal(pubActivity1.Context.SpanId, msg1.GetActivityContext().SpanId);

        var pubActivity2 = await WithActivity(() => nats.PublishAsync("foo", 200, headers: headers.Clone()));
        var msg2 = await signal2;

        Assert.NotNull(pubActivity2);
        Assert.Null(msg2.ReceiveActivity);
        Assert.NotEqual(pubActivity2.TraceId, pubActivity1.TraceId);
        Assert.Equal(pubActivity2.Context.TraceId, msg2.GetActivityContext().TraceId);
        Assert.Equal(pubActivity2.Context.SpanId, msg2.GetActivityContext().SpanId);

        await sub.DisposeAsync();
        await reg;
    }

    [Theory]
    [InlineData(None)]
    [InlineData(CorePublish)]
    [InlineData(CoreReceive)]
    [InlineData(CorePublish | CoreReceive)]
    [InlineData(CorePublish | CoreInternalPublish)]
    [InlineData(CoreReceive | CoreInternalReceive)]
    public async Task PubSubWithTelemetryOnTest(TelemetryFlags enabledTelemetry)
    {
        using var listener = StartActivityListener(GetActivitySources(enabledTelemetry).Append(TestActivities.Name).ToArray());
        await using var server = NatsServer.Start(_output, _transportType);

        await using var nats = server.CreateClientConnection();

        var sync = 0;
        var signal1 = new WaitSignal<NatsMsg<int>>();
        var signal2 = new WaitSignal<NatsMsg<int>>();
        var sub = await nats.SubscribeCoreAsync<int>("foo");
        var reg = sub.Register(async m =>
        {
            if (m.Data < 10)
            {
                Interlocked.Exchange(ref sync, m.Data);
                return;
            }

            if (m.Data == 100)
                signal1.Pulse(m);
            if (m.Data == 200)
            {
                signal2.Pulse(m);
                using var activity = m.StartChildActivity(TestActivities);
                await m.ReplyAsync(201);
            }
        });

        await Retry.Until(
            "subscription is active",
            () => Volatile.Read(ref sync) == 1,
            async () => await nats.PublishAsync("foo", 1));

        var headers = new NatsHeaders { ["Test-Header-Key"] = "test-header-value", ["Multi"] = new[] { "multi-value-0", "multi-value-1" }, };
        Assert.False(headers.IsReadOnly);

        var pubActivity1 = await WithActivity(() => nats.PublishAsync("foo", 100, headers: headers));
        var msg1 = await signal1;

        Assert.NotNull(pubActivity1);
        AssertTracePropagation(pubActivity1, in msg1, enabledTelemetry);

        var (reply, pubActivity2) = await WithActivity(() => nats.RequestAsync<int, int>("foo", 200, headers: headers.Clone()));

        var msg2 = await signal2;

        Assert.NotNull(pubActivity2);
        AssertTracePropagation(pubActivity2, in msg2, enabledTelemetry);
        Assert.Equal(pubActivity2.TraceId, reply.GetActivityContext().TraceId);
        Assert.NotEqual(pubActivity2.SpanId, reply.GetActivityContext().SpanId);

        Assert.NotEqual(pubActivity2.TraceId, pubActivity1.TraceId);
        await sub.DisposeAsync();
        await reg;
    }

    private static async ValueTask<Activity?> WithActivity(Func<ValueTask> action)
    {
        using var activity = TestActivities.StartActivity();
        await action();

        // ReSharper disable once ReturnOfUsingVariable - we want this
        return activity;
    }

    private static async ValueTask<(T Result, Activity?)> WithActivity<T>(Func<ValueTask<T>> action)
    {
        using var activity = TestActivities.StartActivity();

        // ReSharper disable once ReturnOfUsingVariable - we want this
        return (await action(), activity);
    }

    /// <summary>
    /// Asserts correct trace propagation for the activity and received msg based on the enable telemetry.
    /// </summary>
    private static void AssertTracePropagation<T>(Activity beforePublishActivity, in NatsMsg<T> msg, TelemetryFlags enabledTelemetry)
    {
        Assert.Equal(beforePublishActivity.TraceId, msg.GetActivityContext().TraceId);

        if (msg.ReceiveActivity is not null)
        {
            Assert.Equal(msg.ReceiveActivity.TraceId, msg.GetActivityContext().TraceId);
            Assert.Equal(msg.ReceiveActivity.SpanId, msg.GetActivityContext().SpanId);
        }

        if (enabledTelemetry.HasFlag(CoreReceive))
        {
            Assert.NotNull(msg.ReceiveActivity);
            Assert.NotEqual(beforePublishActivity.SpanId, msg.ReceiveActivity.SpanId);

            if (enabledTelemetry.HasFlag(CorePublish))
            {
                Assert.NotEqual(beforePublishActivity.SpanId, msg.ReceiveActivity.ParentSpanId);
            }
            else
            {
                Assert.Equal(beforePublishActivity.SpanId, msg.ReceiveActivity.ParentSpanId);
            }
        }
        else
        {
            Assert.Null(msg.ReceiveActivity);

            if (enabledTelemetry.HasFlag(CorePublish))
            {
                Assert.NotEqual(beforePublishActivity.SpanId, msg.GetActivityContext().SpanId);
            }
            else
            {
                Assert.Equal(beforePublishActivity.SpanId, msg.GetActivityContext().SpanId);
            }
        }
    }
}
