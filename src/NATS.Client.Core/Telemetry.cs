using System.Diagnostics;
using System.Text.RegularExpressions;
using System.Threading.Channels;
using Microsoft.Extensions.Primitives;
using NATS.Client.Core.Commands;
using OpenTelemetry.Trace;

namespace NATS.Client.Core;

// === Spec ====
// - https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/trace/semantic_conventions/messaging.md
public static class Telemetry
{
    public const string ActivitySourceName = "NATS.Client";

    public static ActivitySource NatsActivities { get; } = new(ActivitySourceName);

    internal class Constants
    {
        public const string TraceParent = "traceparent";
        public const string TraceState = "tracestate";
        public const string PublishActivityName = "publish_message";
        public const string ReceiveActivityName = "receive_message";

        // https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/trace/semantic_conventions/messaging.md#messaging-attributes

        public const string SystemKey = OpenTelemetry.Trace.TraceSemanticConventions.AttributeMessagingSystem;
        public const string SystemVal = "nats";
        public const string OpKey = OpenTelemetry.Trace.TraceSemanticConventions.AttributeMessagingOperation;
        public const string OpPub = "publish";
        public const string OpRec = OpenTelemetry.Trace.TraceSemanticConventions.MessagingOperationValues.Receive;
        public const string OpProcess = OpenTelemetry.Trace.TraceSemanticConventions.MessagingOperationValues.Process;

        // destination
        public const string DestNameKey = OpenTelemetry.Trace.TraceSemanticConventions.AttributeMessagingDestination;
        public const string Dest = OpenTelemetry.Trace.TraceSemanticConventions.AttributeMessageType;

        public const string ReplyKey = "messaging.nats.message.reply_to";
        public const string SubjectKey = "messaging.nats.message.subject";


        // public const string SystemKey = OpenTelemetry.Trace.TraceSemanticConventions.AttributeMessagingSystem;
        // public const string SystemKey = OpenTelemetry.Trace.TraceSemanticConventions.AttributeMessagingSystem;
        // public const string SystemKey = OpenTelemetry.Trace.TraceSemanticConventions.AttributeMessagingSystem;

        // rpc ???
        public const string RpcSystemKey = OpenTelemetry.Trace.TraceSemanticConventions.AttributeRpcSystem;
        public const string RpcSystemVal = "nats";


        public string NatsSubject { get; } = "nats.subject";


        // todo: Jetstream
    }

    private static readonly object _boxedTrue = true;

    internal static void SetComplete(ref Activity? activity) => activity?.Dispose();

    internal static void SetException(ref Activity? activity, Exception exception)
    {
        if (activity is null)
            return;

        // see: https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/trace/semantic_conventions/exceptions.md#attributes
        var message = ExceptionUtil.GetMessage(exception);
        var eventTags = new ActivityTagsCollection
        {
            ["exception.escaped"] = _boxedTrue,
            ["exception.type"] = exception.GetType().FullName,
            ["exception.message"] = message,
            ["exception.stacktrace"] = ExceptionUtil.GetStackTrace(exception),
        };

        var activityEvent = new ActivityEvent("exception", DateTimeOffset.UtcNow, eventTags);

        activity.AddEvent(activityEvent);
        activity.SetStatus(ActivityStatusCode.Error, message);
    }

    internal static void FillPublishActivity(ref Activity? activity, ref NatsHeaders? headers, string subject, string? replyTo)
    {
        if (activity is null)
            return;

        // tags
        activity.AddTag(Constants.SystemKey, Constants.SystemVal);
        activity.AddTag(Constants.OpKey, Constants.OpPub);
        activity.AddTag(Constants.DestNameKey, subject);

        if (replyTo is not null)
            activity.AddTag(Constants.ReplyKey, replyTo);

        // headers
        headers ??= new NatsHeaders();
        headers.Add(Constants.TraceParent, GetTraceParent(in activity));

        if (activity.TraceStateString is not null)
            headers.Add(Constants.TraceState, activity.TraceStateString);
    }

    internal static void FillReceiveActivity(ref Activity? activity, NatsHeaders? headers, string subject, string? replyTo)
    {
        if (activity is null)
            return;

        // tags
        activity.AddTag(Constants.SystemKey, Constants.SystemVal);
        activity.AddTag(Constants.OpKey, Constants.OpRec);
        activity.AddTag(Constants.DestNameKey, subject);

        if (replyTo is not null)
            activity.AddTag(Constants.ReplyKey, replyTo);

        if (headers is not null && headers.TryGetValue(Constants.TraceParent, out var traceParent))
        {
            if (!headers.TryGetValue(Constants.TraceState, out var traceState))
                traceState = StringValues.Empty;

            if (ActivityContext.TryParse(traceParent, traceState, out var ctx))
            {
                activity.SetParentId(ctx.TraceId, ctx.SpanId, ctx.TraceFlags);
            }
        }
    }

    private static string GetTraceParent(in Activity activity)
    {
        // from https://github.com/w3c/distributed-tracing/blob/master/trace_context/HTTP_HEADER_FORMAT.md
        // traceparent: 00-0af7651916cd43dd8448eb211c80319c-00f067aa0ba902b7-01
        const string traceParentPrefix = "00-";
        const string traceParentSeparator = "-";

        // ToHexString should just be a ptr copy to internal field
        var traceId = activity.Context.TraceId.ToHexString();
        var spanId = activity.Context.SpanId.ToHexString();
        var traceFlags = (activity.Context.TraceFlags & ActivityTraceFlags.Recorded) != 0
            ? "-01"
            : "-00";

        var parentLen = traceParentPrefix.Length + traceId.Length + traceParentSeparator.Length + spanId.Length + traceFlags.Length;

        // single pass, exact alloc
        return string.Create(length: parentLen, state: (traceId, spanId, traceFlags), static (span, state) =>
        {
            var i = 0;
            traceParentPrefix.CopyTo(span);
            i += traceParentPrefix.Length;

            state.traceId.CopyTo(span[i..]);
            i += state.traceId.Length;

            traceParentSeparator.CopyTo(span[i..]);
            i += traceParentSeparator.Length;

            state.spanId.CopyTo(span[i..]);
            i += state.spanId.Length;

            state.traceFlags.CopyTo(span[i..]);
        });
    }
}

#pragma warning disable SA1400 // incorrect analyzer warning, 'file' is an accessibility modifier
file static class ExceptionUtil
{
    private static readonly Regex Cleanup;

    static ExceptionUtil()
    {
        const string awaiter = @"at System\.Runtime\.CompilerServices\.TaskAwaiter.*";
        const string exception = @"at System\.Runtime\.ExceptionServices\.ExceptionDispatchInfo.*";

        Cleanup = new Regex(
            @"\n\s+(" + string.Join("|", awaiter, exception) + ")+",
            RegexOptions.Multiline | RegexOptions.Compiled);
    }

    public static string GetMessage(Exception exception)
    {
        try
        {
            return exception.Message ?? $"An exception of type {exception.GetType()} was thrown but the message was null.";
        }
        catch
        {
            return $"An exception of type {exception.GetType()} was thrown but the Message property threw an exception.";
        }
    }

    public static string GetStackTrace(Exception? exception)
    {
        var stackTrace = exception?.StackTrace;
        if (string.IsNullOrWhiteSpace(stackTrace))
            return string.Empty;

        return Cleanup.Replace(stackTrace, string.Empty);
    }
}
