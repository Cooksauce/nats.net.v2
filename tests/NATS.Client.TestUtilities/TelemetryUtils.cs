using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text;
using static NATS.Client.TestUtilities.TelemetryFlags;

namespace NATS.Client.TestUtilities;

public static class TelemetryUtils
{
    public static ActivityListener StartActivityListener(params string[] sources) => StartActivityListener(out _, sources);

    public static ActivityListener StartActivityListener(out ConcurrentDictionary<ActivityTraceId, List<Activity>> traces, params string[] sources)
    {
        var tracesLocal = new ConcurrentDictionary<ActivityTraceId, List<Activity>>();
        traces = tracesLocal;
        var sourceSet = new HashSet<string>(sources);
        var listener = new ActivityListener
        {
            ShouldListenTo = s => sourceSet.Contains(s.Name),
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData,
            ActivityStarted = o =>
            {
                if (tracesLocal.TryGetValue(o.TraceId, out var list))
                    list.Add(o);
                else
                    tracesLocal[o.TraceId] = new List<Activity>(capacity: 4) { o };

                Debug.WriteLine($"Started span: {o.DisplayName}... {o.TraceId}");
            },
            ActivityStopped = _ => { },
        };

        ActivitySource.AddActivityListener(listener);
        return listener;
    }

    public static IEnumerable<string> GetActivitySources(TelemetryFlags enabledTelemetry)
    {
        if (enabledTelemetry.HasFlag(CorePublish))
            yield return "NATS.Client.Core.Publish";

        if (enabledTelemetry.HasFlag(CoreReceive))
            yield return "NATS.Client.Core.Receive";

        if (enabledTelemetry.HasFlag(CoreInternalPublish))
            yield return "NATS.Client.Core.Internal.Publish";

        if (enabledTelemetry.HasFlag(CoreInternalReceive))
            yield return "NATS.Client.Core.Internal.Receive";

        if (enabledTelemetry.HasFlag(JSPublish))
            yield return "NATS.Client.JS.Publish";

        if (enabledTelemetry.HasFlag(JSReceive))
            yield return "NATS.Client.JS.Receive";

        if (enabledTelemetry.HasFlag(JSManage))
            yield return "NATS.Client.JS.Manage";

        if (enabledTelemetry.HasFlag(JSInternalPublish))
            yield return "NATS.Client.JS.Internal.Publish";

        if (enabledTelemetry.HasFlag(JSInternalReceive))
            yield return "NATS.Client.JS.Internal.Receive";
    }

    public static string PrintTrace(this IEnumerable<Activity> activities, bool includeSourceName = false)
    {
        var sb = new StringBuilder();
        foreach (var activity in activities)
        {
            if (sb.Length == 0)
                sb.Append($"TRC: {activity.TraceId.ToString()[..7]}... ");
            else
                sb.Append(" --> ");

            sb.Append(activity.DisplayName);

            if (includeSourceName)
                sb.Append($" ({activity.Source.Name.Replace("NATS.Client.", string.Empty)})");
        }

        return sb.ToString();
    }
}
