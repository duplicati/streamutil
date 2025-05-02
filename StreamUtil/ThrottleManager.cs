// Copyright (C) 2025, The Duplicati Team
// https://duplicati.com, hello@duplicati.com
// 
// Permission is hereby granted, free of charge, to any person obtaining a 
// copy of this software and associated documentation files (the "Software"), 
// to deal in the Software without restriction, including without limitation 
// the rights to use, copy, modify, merge, publish, distribute, sublicense, 
// and/or sell copies of the Software, and to permit persons to whom the 
// Software is furnished to do so, subject to the following conditions:
// 
// The above copyright notice and this permission notice shall be included in 
// all copies or substantial portions of the Software.
// 
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS 
// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, 
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE 
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER 
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING 
// FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER 
// DEALINGS IN THE SOFTWARE.

namespace Duplicati.StreamUtil;

using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

/// <summary>
/// Manager class for throttling data transfer rates using a pooled token bucket with proportional fairness.
/// </summary>
public sealed class ThrottleManager : IDisposable
{
    /// <summary>
    /// Represents the state of a single transfer, including bandwidth usage and activity tracking.
    /// </summary>
    private class TransferState
    {
        /// <summary>
        /// Total number of bytes transferred during the current activity window.
        /// </summary>
        public long BytesTransferred;
        /// <summary>
        /// The last time this transfer was active.
        /// </summary>
        public DateTime LastActive = DateTime.UnixEpoch;
        /// <summary>
        /// Stopwatch to track the time since the last reset of transfer usage.
        /// </summary>
        public Stopwatch ActivityStopwatch = Stopwatch.StartNew();
    }

    /// <summary>
    /// Thread-safe collection of active transfer states indexed by their transfer IDs.
    /// </summary>
    private readonly Dictionary<long, TransferState> _transfers = new();
    /// <summary>
    /// Lock object for synchronizing access to shared state.
    /// </summary>
    private readonly object _lock = new();
    /// <summary>
    /// Atomic counter to generate unique transfer IDs.
    /// </summary>
    private long _nextTransferId = 0;
    /// <summary>
    /// The global bandwidth limit in bytes per second.
    /// </summary>
    private double _bytesPerSecond;

    /// <summary>
    /// Maximum number of seconds worth of tokens to accumulate during idle periods.
    /// </summary>
    private const double MaxRefillSeconds = 2.5;
    /// <summary>
    /// The interval after which a transfer's usage weight is reset.
    /// </summary>
    private static readonly TimeSpan ResetInterval = TimeSpan.FromSeconds(10);

    /// <summary>
    /// The number of available tokens in the global pool.
    /// </summary>
    private double _availableTokens = 0;
    /// <summary>
    /// Flag indicating whether the object has been disposed.
    /// </summary>
    private bool _disposed = false;
    /// <summary>
    /// Stopwatch used for tracking elapsed time between token refills.
    /// </summary>
    private readonly Stopwatch _stopwatch = Stopwatch.StartNew();

    /// <summary>
    /// Gets or sets the global bandwidth limit in bytes per second. Setting this value controls the refill rate of the shared token pool.
    /// </summary>
    public long Limit
    {
        get => (long)_bytesPerSecond;
        set
        {
            if (value < 0)
                throw new ArgumentOutOfRangeException(nameof(value));
            if (_disposed)
                throw new ObjectDisposedException(nameof(ThrottleManager));
            lock (_lock)
                _bytesPerSecond = value;
        }
    }

    /// <summary>
    /// Registers a new transfer and returns its unique ID.
    /// </summary>
    /// <returns>A unique ID for the new transfer.</returns>
    public long RegisterTransfer()
    {
        lock (_lock)
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(ThrottleManager));
            var id = Interlocked.Increment(ref _nextTransferId);
            _transfers.TryAdd(id, new TransferState());
            return id;
        }
    }

    /// <summary>
    /// Unregisters a transfer and removes it from tracking.
    /// </summary>
    /// <param name="transferId">The transfer ID to remove.</param>
    public void UnregisterTransfer(long transferId)
    {
        // Ignore, as it may be during shutdown
        if (_disposed)
            return;
        lock (_lock)
        {
            if (_disposed)
                return;
            _transfers.Remove(transferId);
        }
    }

    /// <summary>
    /// Disposes the throttle manager and clears all internal state.
    /// </summary>
    public void Dispose()
    {
        lock (_lock)
        {
            _disposed = true;
            _transfers.Clear();
        }
    }

    /// <summary>
    /// Sleeps the calling thread if necessary to honor the bandwidth limit.
    /// </summary>
    /// <param name="transferId">The ID of the requesting transfer.</param>
    /// <param name="size">The number of bytes to transfer.</param>
    public void SleepForSize(long transferId, long size)
    {
        var delay = RequestTokens(transferId, size);
        if (delay > TimeSpan.Zero)
            Thread.Sleep(delay);
    }

    /// <summary>
    /// Asynchronously waits if necessary to honor the bandwidth limit.
    /// </summary>
    /// <param name="transferId">The ID of the requesting transfer.</param>
    /// <param name="size">The number of bytes to transfer.</param>
    /// <param name="cancellationToken">Cancellation token to cancel the wait.</param>
    /// <returns>A task that completes after the required delay.</returns>
    public Task WaitForSize(long transferId, long size, CancellationToken cancellationToken)
    {
        var delay = RequestTokens(transferId, size);
        return delay == TimeSpan.Zero ? Task.CompletedTask : Task.Delay(delay, cancellationToken);
    }

    /// <summary>
    /// Internal method to calculate delay needed for the given size based on pooled tokens.
    /// </summary>
    /// <param name="transferId">The transfer making the request.</param>
    /// <param name="size">The size of the data in bytes.</param>
    /// <returns>The time to wait if the request exceeds the fair share.</returns>
    private TimeSpan RequestTokens(long transferId, long size)
    {
        if (_bytesPerSecond <= 0 || size <= 0)
            return TimeSpan.Zero;

        lock (_lock)
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(ThrottleManager));

            var now = DateTime.UtcNow;
            var elapsed = _stopwatch.Elapsed.TotalSeconds;
            var maxTokens = _bytesPerSecond * MaxRefillSeconds;
            _availableTokens = Math.Min(_availableTokens + (_bytesPerSecond * elapsed), maxTokens);
            _stopwatch.Restart();

            // Get the current state and ignore if we don't know the requester
            if (!_transfers.TryGetValue(transferId, out var requester))
                return TimeSpan.Zero;

            // Update active stats and compute total usage
            var reset = false;
            requester.LastActive = now;
            if (requester.ActivityStopwatch.Elapsed > ResetInterval)
            {
                requester.BytesTransferred = 0;
                requester.ActivityStopwatch.Restart();
                reset = true;
            }

            var weight = requester.BytesTransferred;

            // Find the total weight of all active transfers
            var totalWeight = _transfers.Values
                .Where(t => t.LastActive > now - ResetInterval)
                .Sum(t => t.BytesTransferred);

            // If no one is active, this request gets all available tokens
            if (totalWeight == 0)
            {
                totalWeight = 1;
                weight = 1;
                // From a clean state, there are no tokens available
                if (!reset)
                    _availableTokens = Math.Min(_availableTokens, 0);
            }

            var fairShare = _availableTokens * (weight / (double)totalWeight);
            _availableTokens -= size;
            requester.BytesTransferred += size;

            var delay = size > fairShare ? TimeSpan.FromSeconds((size - fairShare) / _bytesPerSecond) : TimeSpan.Zero;
            return delay;
        }
    }
}
