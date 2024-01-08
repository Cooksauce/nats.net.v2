using System.Diagnostics;
using Microsoft.Extensions.Logging;
using NATS.Client.Core;
using NATS.Client.Serializers.Json;
using OpenTelemetry;
using OpenTelemetry.Exporter;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;

Environment.SetEnvironmentVariable("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:16161");

using var tracerProvider = StartTracing();

var subject = "bar.*";
var options = NatsOpts.Default with
{
    LoggerFactory = LoggerFactory.Create(builder => builder.AddConsole()),
    SerializerRegistry = NatsJsonSerializerRegistry.Default,
};

Print("[CON] Connecting...\n");

await using var connection = new NatsConnection(options);

Print($"[SUB] Subscribing to subject '{subject}'...\n");

await foreach (var msg in connection.SubscribeAsync<Bar>(subject))
{
    var activity = Activity.Current;
    Print($"[RCV] {msg.Subject} (Act: {activity?.OperationName}): {msg.Data}\n");
}

void Print(string message)
{
    Console.Write($"{DateTime.Now:HH:mm:ss} {message}");
}

static TracerProvider StartTracing() => new TracerProviderBuilderBase()
        // .AddNatsInstrumentation()
        // .AddHttpClientInstrumentation()
        .AddSource(Telemetry.NatsActivitySource)
        .ConfigureResource(o => o
            .AddTelemetrySdk()
            .AddService(serviceName: typeof(Program).Assembly.GetName().Name!))
        .AddOtlpExporter(o => o.ExportProcessorType = ExportProcessorType.Batch)
        .AddConsoleExporter(o => o.Targets = ConsoleExporterOutputTargets.Console)
        .Build()
    ?? throw new Exception("Tracer provider build returned null.");

public record Bar
{
    public int Id { get; set; }

    public string? Name { get; set; }
}
