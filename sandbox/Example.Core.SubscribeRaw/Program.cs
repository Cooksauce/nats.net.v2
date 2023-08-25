using System.Diagnostics;
using System.Text;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using NATS.Client.Core;
using OpenTelemetry;
using OpenTelemetry.Exporter;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;

using var trace = StartTracing();

using var listener = new ActivityListener
{
    ActivityStarted = o => Console.WriteLine($"Started {o.OperationName} {o.Id}"),
    ActivityStopped = o => Console.WriteLine($"Stopped {o.OperationName} {o.Id}"),
    ShouldListenTo = o => o.Name == Telemetry.ActivitySourceName,
};

ActivitySource.AddActivityListener(listener);

var subject = "foo.*";
var options = NatsOptions.Default with { LoggerFactory = new MinimumConsoleLoggerFactory(LogLevel.Error) };

Print("[CON] Connecting...\n");
using var t = Telemetry.NatsActivities.StartActivity("Subscribing");

await using var connection = new NatsConnection(options);

Print($"[SUB] Subscribing to subject '{subject}'...\n");

var sub = await connection.SubscribeAsync(subject);

t.Dispose();

await Task.WhenAll(Enumerable.Range(0, 3).Select(_ =>
    ReadMessages(sub.Msgs, connection, default)));

// await foreach (var msg in sub.Msgs.ReadAllAsync())
// {
//     using var _ = Activity.Current = msg.Activity;
//     using var consumeActivity = Telemetry.NatsActivities.StartActivity("process");
//     var data = Encoding.UTF8.GetString(msg.Data.ToArray());
//     Print($"[RCV] {msg.Subject}: {data}\n");
//     await Task.Delay(80);
// }

async Task ReadMessages(ChannelReader<NatsMsg> msgs, NatsConnection connection, CancellationToken ct)
{
    while (true)
    {
        try
        {
            var msg = await msgs.ReadAsync(ct);
            using var _ = Activity.Current = msg.Activity;
            using var consumeActivity = Telemetry.NatsActivities.StartActivity("process");
            var data = Encoding.UTF8.GetString(msg.Data.ToArray());
            Print($"[RCV] {msg.Subject}: {data}\n");
            await Task.Delay(300, ct);

            await connection.PublishAsync(
                msg with { Subject = "def" },
                cancellationToken: ct);
        }
        catch (Exception e)
        {
            Console.WriteLine(e);
        }
    }
}

void Print(string message)
{
    Console.Write($"{DateTime.Now:HH:mm:ss} {message}");
}

trace.ForceFlush();

static TracerProvider StartTracing() => new TracerProviderBuilderBase()
    .AddSource(Telemetry.ActivitySourceName)
    .ConfigureResource(o => o
        .AddTelemetrySdk()
        .AddService(serviceName: "subscribe-raw"))
    .AddOtlpExporter(o => o.ExportProcessorType = ExportProcessorType.Batch)
    // .AddConsoleExporter(o => o.Targets = ConsoleExporterOutputTargets.Console)
    .Build()
    ?? throw new Exception("Tracer provider build returned null.");
