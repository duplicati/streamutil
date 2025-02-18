// Copyright (C) 2024, The Duplicati Team
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

/// <summary>
/// Manager class for throttling data transfer rates.
/// </summary>
public sealed class ThrottleManager
{
    /// <summary>
    /// Minimum time interval to measure data transfer rate over.
    /// </summary>
    private readonly long MinimumMeasureInterval = TimeSpan.FromMilliseconds(1).Ticks;
    /// <summary>
    /// Minimum time interval to wait between measurements.
    /// </summary>
    private readonly long MinimumWaitInterval = TimeSpan.FromMilliseconds(1).Ticks;
    /// <summary>
    /// The interval between each reset
    /// </summary>
    private readonly long MeasureWindowInterval = TimeSpan.FromSeconds(1).Ticks;
    /// <summary>
    /// The start time of the current period.
    /// </summary>
    private long _periodStart = DateTime.Now.Ticks;
    /// <summary>
    /// The amount of data transferred in the current period.
    /// </summary>
    private long _periodCount = 0;
    /// <summary>
    /// Lock object for thread safety.
    /// </summary>
    private readonly object _lock = new();

    /// <summary>
    /// The maximum data transfer rate in bytes per second.
    /// </summary>
    private long _limit = 0;

    /// <summary>
    /// Gets or sets the maximum data transfer rate in bytes per second.
    /// </summary>
    public long Limit
    {
        get => _limit;
        set
        {
            if (value < 0)
                throw new ArgumentOutOfRangeException(nameof(value));

            lock (_lock)
            {
                _limit = value;
                ResetPeriod();
            }
        }
    }

    /// <summary>
    /// Delays the current request if the data transfer rate exceeds the limit.
    /// </summary>
    /// <param name="size">The size of the data to transfer.</param>
    /// <returns>The time to wait before or after completing the operation.</returns>
    public TimeSpan DelayForSize(long size)
    {
        if (size == 0 || _limit == 0)
            return TimeSpan.Zero;

        lock (_lock)
            _periodCount += size;

        var elapsedTicks = DateTime.Now.Ticks - _periodStart;
        if (elapsedTicks < MinimumMeasureInterval)
            return TimeSpan.Zero;

        var bps = _periodCount * TimeSpan.TicksPerSecond / elapsedTicks;
        if (bps > _limit)
        {
            var delay = (_periodCount * TimeSpan.TicksPerSecond / _limit) - elapsedTicks;
            if (delay > MinimumWaitInterval)
            {
                if (elapsedTicks > MeasureWindowInterval)
                    ResetPeriod(delay);
                return TimeSpan.FromTicks(delay);
            }
        }

        // If the transfer is slower than the limit, reset the period once it has elapsed
        if (elapsedTicks > MeasureWindowInterval)
            ResetPeriod();

        return TimeSpan.Zero;
    }

    /// <summary>
    /// Delays the current request if the data transfer rate exceeds the limit.
    /// </summary>
    /// <param name="size">The size of the data to transfer.</param>
    public void SleepForSize(long size)
    {
        var delay = DelayForSize(size);
        if (delay != TimeSpan.Zero)
            Thread.Sleep(delay);
    }

    /// <summary>
    /// Waits for the data transfer rate to drop below the limit.
    /// </summary>
    /// <param name="size">The size of the data to transfer.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A task that completes when the data transfer rate is below the limit.</returns>
    public Task WaitForSize(long size, CancellationToken cancellationToken)
    {
        var delay = DelayForSize(size);
        if (delay == TimeSpan.Zero)
            return Task.CompletedTask;

        return Task.Delay(delay, cancellationToken);
    }

    /// <summary>
    /// Resets the data transfer rate measurement period.
    /// </summary>
    public void ResetPeriod()
        => ResetPeriod(0);

    /// <summary>
    /// Resets the data transfer rate measurement period.
    /// </summary>
    /// <param name="sleepOffset">The amount of time to sleep before resetting the period.</param>
    private void ResetPeriod(long sleepOffset)
    {
        lock (_lock)
        {
            _periodStart = DateTime.Now.Ticks + sleepOffset;
            _periodCount = 0;
        }
    }
}
