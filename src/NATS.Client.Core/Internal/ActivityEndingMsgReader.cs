using System.Runtime.CompilerServices;
using System.Threading.Channels;

namespace NATS.Client.Core.Internal;

internal sealed class ActivityEndingMsgReader<T> : ChannelReader<NatsMsg<T>>
{
    private readonly ChannelReader<NatsMsg<T>> _inner;

    public ActivityEndingMsgReader(ChannelReader<NatsMsg<T>> inner) => _inner = inner;

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override bool TryRead(out NatsMsg<T> item)
    {
        if (_inner.TryRead(out item))
            item.Activity?.Dispose();

        return false;
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override ValueTask<bool> WaitToReadAsync(CancellationToken cancellationToken = default) => _inner.WaitToReadAsync(cancellationToken);
}
