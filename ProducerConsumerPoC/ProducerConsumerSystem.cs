namespace ProducerConsumerPoC;

using System.Threading.Channels;

public sealed class ProducerConsumerSystem<T> : IAsyncDisposable
{
    private readonly Channel<T> _channel;
    private readonly Task[] _consumers;
    private readonly Func<T, ValueTask> _handler;
    private readonly CancellationTokenSource _stopCts = new();
    private bool _started;
    private bool _disposed;

    public ProducerConsumerSystem(int consumerCount, int capacity, Func<T, ValueTask> handler)
    {
        if (consumerCount < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(consumerCount), "Consumer count must be at least 1.");
        }

        if (capacity < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(capacity), "Queue capacity must be at least 1.");
        }

        _handler = handler ?? throw new ArgumentNullException(nameof(handler));

        var options = new BoundedChannelOptions(capacity)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = false,
            SingleWriter = false
        };

        _channel = Channel.CreateBounded<T>(options);
        _consumers = new Task[consumerCount];
    }

    public Task StartAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();

        if (_started)
        {
            return Task.CompletedTask;
        }

        _started = true;

        for (var i = 0; i < _consumers.Length; i++)
        {
            _consumers[i] = Task.Run(() => ConsumerLoopAsync(i, cancellationToken), CancellationToken.None);
        }

        return Task.CompletedTask;
    }

    public async Task EnqueueAsync(T item, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        await _channel.Writer.WriteAsync(item, cancellationToken).ConfigureAwait(false);
    }

    public async Task StopAsync()
    {
        ThrowIfDisposed();

        if (!_started)
        {
            return;
        }

        _channel.Writer.Complete();
        _stopCts.Cancel();

        await Task.WhenAll(_consumers).ConfigureAwait(false);
    }

    private async Task ConsumerLoopAsync(int consumerId, CancellationToken cancellationToken)
    {
        var linkedTokenSource = CancellationTokenSource.CreateLinkedTokenSource(_stopCts.Token, cancellationToken);
        var token = linkedTokenSource.Token;

        try
        {
            while (await _channel.Reader.WaitToReadAsync(token).ConfigureAwait(false))
            {
                while (_channel.Reader.TryRead(out var item))
                {
                    await _handler(item).ConfigureAwait(false);
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Graceful shutdown requested.
        }
        finally
        {
            linkedTokenSource.Dispose();
        }
    }

    private void ThrowIfDisposed()
    {
        if (_disposed)
        {
            throw new ObjectDisposedException(nameof(ProducerConsumerSystem<T>));
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        _stopCts.Cancel();
        _channel.Writer.TryComplete();

        try
        {
            await Task.WhenAll(_consumers.Where(t => t != null)).ConfigureAwait(false);
        }
        catch
        {
            // Ignore exceptions during disposal.
        }
        finally
        {
            _stopCts.Dispose();
        }
    }
}
