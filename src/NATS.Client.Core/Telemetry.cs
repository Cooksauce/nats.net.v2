using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.Primitives;

namespace NATS.Client.Core;

// https://opentelemetry.io/docs/specs/semconv/attributes-registry/messaging/
// https://opentelemetry.io/docs/specs/semconv/messaging/messaging-spans/#messaging-attributes

public static class Telemetry
{
    public static string NatsActivitySource = "NATS.Client";
    private static readonly object BoxedTrue = true;
    private static readonly ActivitySource NatsActivities = new(name: NatsActivitySource);

    internal class Constants
    {
        public const string True = "true";
        public const string False = "false";
        public const string TraceParent = "traceparent";
        public const string TraceState = "tracestate";
        public const string PublishActivityName = "publish_message";
        public const string ReceiveActivityName = "receive_message";

        // https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/trace/semantic_conventions/messaging.md#messaging-attributes

        public const string SystemKey = "messaging.system";
        public const string SystemVal = "nats";
        public const string ClientId = "messaging.client_id";
        public const string OpKey = "messaging.operation";
        public const string OpPub = "publish";
        public const string OpRec = "receive";
        public const string MsgBodySize = "messaging.message.body.size";
        public const string MsgTotalSize = "messaging.message.envelope.size";
        public const string OpProcess = "process"; // OpenTelemetry.Trace.TraceSemanticConventions.MessagingOperationValues.Process;

        // destination
        public const string DestTemplate = "messaging.destination.template";
        public const string DestName = "messaging.destination.name";
        public const string DestIsTemporary = "messaging.destination.temporary";
        public const string DestIsAnon = "messaging.destination.anonymous";
        public const string DestPubName = "messaging.destination_publish.name";
        public const string DestPubIsAnon = "messaging.destination_publish.anonymous";


        // public const string Dest = OpenTelemetry.Trace.TraceSemanticConventions.AttributeMessageType;

        public const string ReplyTo = "messaging.nats.message.reply_to";
        public const string Subject = "messaging.nats.message.subject";


        // public const string SystemKey = OpenTelemetry.Trace.TraceSemanticConventions.AttributeMessagingSystem;
        // public const string SystemKey = OpenTelemetry.Trace.TraceSemanticConventions.AttributeMessagingSystem;
        // public const string SystemKey = OpenTelemetry.Trace.TraceSemanticConventions.AttributeMessagingSystem;

        // rpc ???
        public const string RpcSystemKey = "rpc.system";
        public const string RpcSystemVal = "nats";


        // todo: Jetstream
    }

    [SuppressMessage("ReSharper", "ExplicitCallerInfoArgument", Justification = "We want constant activity name.")]
    internal static Activity? StartSendActivity(INatsConnection? connection, string subject, string? replyTo, ref NatsHeaders? headers)
    {
        if (NatsActivities.StartActivity(Constants.PublishActivityName, ActivityKind.Producer) is not { } activity)
            return null; // todo: should propagate if not subscribed to activity source???

        if (connection is NatsConnection { ServerInfo: not null } conn)
            activity.AddTag(Constants.ClientId, conn.ServerInfo.ClientId.ToString());

        // tags
        activity.AddTag(Constants.SystemKey, Constants.SystemVal);
        activity.AddTag(Constants.OpKey, Constants.OpPub);
        activity.AddTag(Constants.DestName, subject);

        if (replyTo is not null)
            activity.AddTag(Constants.ReplyTo, replyTo);

        // headers
        headers ??= new NatsHeaders();
        DistributedContextPropagator.Current.Inject(
            activity,
            headers,
            static (carrier, fieldName, fieldValue) =>
            {
                if (carrier is not NatsHeaders headers)
                {
                    Debug.Assert(false, "This should never be hit.");
                    return;
                }

                headers.Add(fieldName, fieldValue);
            });

        // headers.Add(Constants.TraceParent, GetTraceParent(in activity));
        //
        // if (activity.TraceStateString is not null)
        //     headers.Add(Constants.TraceState, activity.TraceStateString);

        return activity;
    }

    [SuppressMessage("ReSharper", "ExplicitCallerInfoArgument", Justification = "We want constant activity name.")]
    internal static Activity? CreateReceiveActivity(INatsConnection? connection, string subscriptionSubject)
    {
        if (NatsActivities.CreateActivity(Constants.ReceiveActivityName, ActivityKind.Consumer) is not { } activity)
            return null;

        if (connection is NatsConnection { ServerInfo: not null } conn)
            activity.AddTag(Constants.ClientId, conn.ServerInfo.ClientId.ToString());

        activity.AddTag(Constants.SystemKey, Constants.SystemVal);
        activity.AddTag(Constants.OpKey, Constants.OpRec);
        activity.AddTag(Constants.DestTemplate, subscriptionSubject);

        return activity;
    }

    internal static void AddBodySize(Activity? activity, long bodySize) => activity?.AddTag(Constants.MsgBodySize, bodySize.ToString());

    internal static void AddMsg<T>(Activity? activity, NatsMsg<T> msg)
    {
        if (activity is null)
            return;

        activity.AddTag(Constants.Subject, msg.Subject);
        activity.AddTag(Constants.DestName, msg.Subject);
        activity.AddTag(Constants.DestPubName, msg.Subject);
        // activity.AddTag(Constants.DestIsTemp, SubjectIsTemp ? Constants.True : Constants.False);
        // activity.AddTag(Constants.DestIsAnon, SubjectIsAnon ? Constants.True : Constants.False);
        activity.AddTag(Constants.MsgTotalSize, msg.Size.ToString());

        if (msg.ReplyTo is not null)
            activity.AddTag(Constants.ReplyTo, msg.ReplyTo);
    }

    internal static void FillTraceContext(Activity? activity, NatsHeaders? headers)
    {
        if (activity is null)
            return;

        if (headers is null)
        {
            activity.Start();
            return;
        }

        DistributedContextPropagator.Current.ExtractTraceIdAndState(
            headers,
            getter: static (object? carrier, string fieldName, out string? fieldValue, out IEnumerable<string>? fieldValues) =>
            {
                if (carrier is not NatsHeaders headers)
                {
                    Debug.Assert(false, "This should never be hit.");
                    fieldValue = null;
                    fieldValues = null;
                    return;
                }

                if (headers.TryGetValue(fieldName, out var values))
                {
                    if (values.Count == 1)
                    {
                        fieldValue = values[0];
                        fieldValues = null;
                    }
                    else
                    {
                        fieldValue = null;
                        fieldValues = values;
                    }
                }
                else
                {
                    fieldValue = null;
                    fieldValues = null;
                }
            },
            out var traceParent,
            out var traceState);

        // var parentSpan = traceState.AsSpan();
        // var tIdRaw = parentSpan[3..35];
        // var sIdRaw = parentSpan[35..];
        // var traceId = ActivityTraceId.CreateFromString(tIdRaw);
        // var spanId = ActivitySpanId.CreateFromString(sIdRaw);

        if (ActivityContext.TryParse(traceParent, traceState, out var ctx))
        {
            activity.SetParentId(ctx.TraceId, ctx.SpanId, ctx.TraceFlags);
        }

        activity.Start();

        // if (headers is not null && headers.TryGetValue(Constants.TraceParent, out var traceParent))
        // {
        //     if (!headers.TryGetValue(Constants.TraceState, out var traceState))
        //         traceState = StringValues.Empty;
        //
        //     if (ActivityContext.TryParse(traceParent, traceState, out var ctx))
        //     {
        //         activity.SetParentId(ctx.TraceId, ctx.SpanId, ctx.TraceFlags);
        //     }
        // }
    }

    internal static void SetException(Activity? activity, Exception exception)
    {
        if (activity is null)
            return;

        // see: https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/trace/semantic_conventions/exceptions.md#attributes
        var message = GetMessage(exception);
        var eventTags = new ActivityTagsCollection
        {
            ["exception.escaped"] = BoxedTrue,
            ["exception.type"] = exception.GetType().FullName,
            ["exception.message"] = message,
            ["exception.stacktrace"] = GetStackTrace(exception),
        };

        var activityEvent = new ActivityEvent("exception", DateTimeOffset.UtcNow, eventTags);

        activity.AddEvent(activityEvent);
        activity.SetStatus(ActivityStatusCode.Error, message);
        return;

        static string GetMessage(Exception exception)
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

        static string GetStackTrace(Exception? exception)
        {
            var stackTrace = exception?.StackTrace;
            if (string.IsNullOrWhiteSpace(stackTrace))
                return string.Empty;

            return stackTrace;
            // todo: return Cleanup.Replace(stackTrace, string.Empty);
        }
    }

    private static string GetTraceParent(in Activity activity)
    {
        // from https://github.com/w3c/distributed-tracing/blob/master/trace_context/HTTP_HEADER_FORMAT.md
        // traceparent: 00-0af7651916cd43dd8448eb211c80319c-00f067aa0ba902b7-01
        const string traceParentPrefix = "00-";
        const string traceParentSeparator = "-";

        // todo: replace with DistributedContextPropagator

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
