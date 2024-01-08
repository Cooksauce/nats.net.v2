// > nats sub bar.*
using Microsoft.Extensions.Logging;
using NATS.Client.Core;
using NATS.Client.Serializers.Json;
using OpenTelemetry;
using OpenTelemetry.Exporter;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;

Environment.SetEnvironmentVariable("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:16161");

using var tracerProvider = StartTracing();

var subject = "bar.xyz";
var options = NatsOpts.Default with
{
    LoggerFactory = LoggerFactory.Create(builder => builder.AddConsole()),
    SerializerRegistry = NatsJsonSerializerRegistry.Default,
};

Print("[CON] Connecting...\n");

await using var connection = new NatsConnection(options);

for (var i = 0; i < 1; i++)
{
    Print($"[PUB] Publishing to subject ({i}) '{subject}'...\n");
    await connection.PublishAsync(subject, new Bar { Id = i, Name = "Baz" });
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
