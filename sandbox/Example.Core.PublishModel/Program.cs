// > nats sub bar.*

using System.Diagnostics;
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
    ShouldListenTo = o => true, // o.Name == Telemetry.ActivitySourceName,
};

ActivitySource.AddActivityListener(listener);

var subject = "foo.xyz";
var options = NatsOptions.Default with
{
    LoggerFactory = new MinimumConsoleLoggerFactory(LogLevel.Error),
    WriterCommandBufferLimit = 3,
};

using var t = Telemetry.NatsActivities.StartActivity("Connecting", ActivityKind.Producer);

Print("[CON] Connecting...\n");

await using var connection = new NatsConnection(options);

t?.Dispose();

for (var i = 0; i < 2; i++)
{
    Print($"[PUB] Publishing to subject ({i}) '{subject}'...\n");
    await connection.PublishAsync<Bar>(subject, new Bar { Id = i, Name = "Baz" });
}

void Print(string message)
{
    Console.Write($"{DateTime.Now:HH:mm:ss} {message}");
}

trace.ForceFlush();

static TracerProvider StartTracing() => new TracerProviderBuilderBase()
    .AddSource(Telemetry.ActivitySourceName)
    .AddOtlpExporter(o => o.ExportProcessorType = ExportProcessorType.Simple)
    //.AddConsoleExporter(o => o.Targets = ConsoleExporterOutputTargets.Console)
    .ConfigureResource(o => o
        .AddTelemetrySdk()
        .AddService(serviceName: "publish-model"))
    .Build()
    ?? throw new Exception("Tracer provider build returned null.");

public record Bar
{
    public int Id { get; set; }

    public string? Name { get; set; }
}




