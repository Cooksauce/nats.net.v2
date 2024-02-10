using System.Diagnostics;
using NATS.Client.Core.Tests;
using NATS.Client.TestUtilities;
using static NATS.Client.TestUtilities.TelemetryFlags;
using static NATS.Client.TestUtilities.TelemetryUtils;

namespace NATS.Client.JetStream.Tests;

public class JSTelemetryTests
{
    private static readonly ActivitySource TestActivities = new("Tests.NATS.Client.JS");

    private readonly ITestOutputHelper _output;

    public JSTelemetryTests(ITestOutputHelper output) => _output = output;

    [Theory]
    [InlineData(None)]
    [InlineData(All)]
    [InlineData(JSPublish)]
    [InlineData(JSReceive)]
    [InlineData(JSPublish | JSReceive)]
    [InlineData(JSPublish | JSReceive | CorePublish | CoreReceive)]
    [InlineData(JSPublish | JSReceive | JSManage | CorePublish | CoreReceive)]
    [InlineData(JSPublish | CoreInternalPublish)]
    [InlineData(JSReceive | CoreInternalReceive)]
    [InlineData(JSPublish | JSReceive | CoreInternalPublish | CoreInternalReceive)]
    public async Task ConsumeWithTelemetryOnTest(TelemetryFlags enabledTelemetry)
    {
        using var listener = StartActivityListener(out var traces, GetActivitySources(enabledTelemetry).Append(TestActivities.Name).ToArray());

        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await using var server = NatsServer.StartJS();
        await using var nats = server.CreateClientConnection();
        var js = new NatsJSContext(nats);

        var stream = await js.CreateStreamAsync("s1", new[] { "s1.*" }, cts.Token);
        var pubActivities = new Dictionary<int, Activity?>();

        for (var i = 0; i < 10; i++)
        {
            var activity = await WithActivity(async () => await js.PublishAsync("s1.foo", i, cancellationToken: cts.Token));
            pubActivities.Add(i, activity);
        }

        var consumer = await stream.CreateOrderedConsumerAsync(cancellationToken: cts.Token);

        var count = 0;
        _output.WriteLine("Consuming...");
        var consumeOpts = new NatsJSConsumeOpts
        {
            MaxMsgs = 3,
            Expires = TimeSpan.FromSeconds(3),
        };

        var idx = 0;
        await foreach (var msg in consumer.ConsumeAsync<int>(opts: consumeOpts, cancellationToken: cts.Token))
        {
            Assert.True(traces.Remove(msg.GetActivityContext().TraceId, out var trace));
            _output.WriteLine($"[RCV] {msg.Data} - {trace.PrintTrace(true)}");
            Assert.Equal(count, msg.Data);
            if (++count == 10)
                break;

            var pubActivity = pubActivities[idx++];
            Assert.NotNull(pubActivity);
            Assert.Equal(pubActivity.Context.TraceId, msg.GetActivityContext().TraceId);
            AssertTracePropagation(pubActivity, msg.Msg, enabledTelemetry);
        }

        var orderedTraces = traces.Values
            .OrderBy(o => o.MinBy(x => x.StartTimeUtc)?.StartTimeUtc ?? DateTime.MinValue);

        _output.WriteLine("Stranded Traces:");
        foreach (var trace in orderedTraces)
            _output.WriteLine($"{trace.PrintTrace(true)}");

        Assert.Empty(traces);
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

        if (enabledTelemetry.HasFlag(JSReceive) || enabledTelemetry.HasFlag(CoreInternalReceive))
        {
            Assert.NotNull(msg.ReceiveActivity);
            Assert.NotEqual(beforePublishActivity.SpanId, msg.ReceiveActivity.SpanId);

            if (enabledTelemetry.HasFlag(JSPublish) ||
                enabledTelemetry.HasFlag(CoreInternalPublish))
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

            if (enabledTelemetry.HasFlag(JSPublish) ||
                enabledTelemetry.HasFlag(CoreInternalPublish))
            {
                Assert.NotEqual(beforePublishActivity.SpanId, msg.GetActivityContext().SpanId);
            }
            else
            {
                Assert.Equal(beforePublishActivity.SpanId, msg.GetActivityContext().SpanId);
            }
        }
    }

    private static async ValueTask<Activity?> WithActivity(Func<ValueTask> action)
    {
        using var activity = TestActivities.StartActivity();
        await action();

        // ReSharper disable once ReturnOfUsingVariable - we want this
        return activity;
    }
}
