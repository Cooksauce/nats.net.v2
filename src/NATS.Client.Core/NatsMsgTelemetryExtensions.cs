using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace NATS.Client.Core;

public static class NatsMsgTelemetryExtensions
{
    /// <summary>
    /// Get the activity context associated with a <see cref="NatsMsg{T}"/>.
    /// </summary>
    /// <param name="msg">Message associated with the desired context.</param>
    /// <typeparam name="T">Associated data type on the <paramref name="msg"/>.</typeparam>
    /// <returns>
    /// Returns the <see cref="ActivityContext"/> associated with receiving this nats message if present; otherwise, the context propagated in the message itself.
    /// The value will be default if no activity is present.
    /// </returns>
    /// <remarks>
    /// This is the context that should be used to create new activities which are continuations of this tracing context.
    /// </remarks>
    public static ActivityContext GetActivityContext<T>(this in NatsMsg<T> msg)
        => msg.Headers?.ActivityContext ?? default;

    /// <summary>
    /// Starts a new child activity under the tracing context associated with the given <paramref name="msg"/>.
    /// </summary>
    /// <param name="msg">Message associated with the desired context</param>
    /// <param name="activitySource">Source which owns the new activity</param>
    /// <param name="name">Name of the new activity</param>
    /// <param name="tags">Tags to add to the new activity</param>
    /// <typeparam name="T">Associated data type on the <paramref name="msg"/></typeparam>
    /// <seealso cref="GetActivityContext{T}"/>
    public static Activity? StartChildActivity<T>(this in NatsMsg<T> msg, ActivitySource activitySource, [CallerMemberName] string name = "", IEnumerable<KeyValuePair<string, object?>>? tags = null)
        => activitySource.StartActivity(
            name,
            kind: ActivityKind.Internal,
            parentContext: GetActivityContext(in msg),
            tags: tags);

    /// <summary>
    /// Starts a new child activity under the tracing context associated with the given <paramref name="msg"/>.
    /// </summary>
    /// <param name="activitySource">Source which owns the new activity</param>
    /// <param name="msg">Message associated with the desired context</param>
    /// <param name="name">Name of the new activity</param>
    /// <param name="tags">Tags to add to the new activity</param>
    /// <typeparam name="T">Associated data type on the <paramref name="msg"/></typeparam>
    /// <seealso cref="GetActivityContext{T}"/>
    public static Activity? StartChildActivity<T>(this ActivitySource activitySource, in NatsMsg<T> msg, [CallerMemberName] string name = "", IEnumerable<KeyValuePair<string, object?>>? tags = null)
        => activitySource.StartActivity(
            name,
            kind: ActivityKind.Internal,
            parentContext: GetActivityContext(in msg),
            tags: tags);

    /// <summary>
    /// Tries to get the activity context associated with a <see cref="NatsMsg{T}"/>.
    /// </summary>
    /// <param name="msg">Message associated with the desired context.</param>
    /// <param name="activityContext">Context associated with the <paramref name="msg"/>, or undefined if none present.</param>
    /// <typeparam name="T">Associated data type on the <paramref name="msg"/>.</typeparam>
    /// <returns>Returns true if an associated activity context exists, otherwise false.</returns>
    public static bool TryGetActivityContext<T>(this in NatsMsg<T> msg, out ActivityContext activityContext)
    {
        activityContext = GetActivityContext(in msg);
        return activityContext != default;
    }
}
