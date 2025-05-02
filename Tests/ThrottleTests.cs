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

using System.Diagnostics;
using Duplicati.StreamUtil;

namespace Tests;

public class ThrottleTests
{
    [Test]
    [TestCase(1000, 10, 0.05)]
    public async Task ThrottleStream(int testSizeMB, int throttleMBs, double delta)
    {
        var source = new MemoryStream();
        source.SetLength(1024 * 1024 * testSizeMB);
        var target = new MemoryStream();

        var throttleManager = new ThrottleManager
        {
            Limit = 1024 * 1024 * throttleMBs
        };
        var throttledStream = new ThrottleEnabledStream(source, throttleManager);

        var start = DateTime.Now;
        await throttledStream.CopyToAsync(target);
        var elapsed = DateTime.Now - start;

        var targetTime = TimeSpan.FromSeconds(source.Length / throttleManager.Limit);

        if (Math.Abs((elapsed - targetTime).TotalSeconds) > targetTime.TotalSeconds * delta)
            Assert.Fail($"Elapsed time {elapsed} is not within {delta * 100}% of target time {targetTime}");
    }

    [Test]
    [TestCase(100, 10, 0.05)]
    public async Task ThrottleStreamWithPause(int testSizeMB, int throttleMBs, double delta)
    {
        var source = new MemoryStream();
        source.SetLength(1024 * 1024 * testSizeMB);
        var target = new MemoryStream();

        var throttleManager = new ThrottleManager
        {
            Limit = 1024 * 1024 * throttleMBs
        };
        var throttledStream = new ThrottleEnabledStream(source, throttleManager);

        var start = DateTime.Now;
        await throttledStream.CopyToAsync(target);
        var elapsed = DateTime.Now - start;

        var targetTime = TimeSpan.FromSeconds(source.Length / throttleManager.Limit);

        if (Math.Abs((elapsed - targetTime).TotalSeconds) > targetTime.TotalSeconds * delta)
            Assert.Fail($"Elapsed time {elapsed} is not within {delta * 100}% of target time {targetTime}");

        throttledStream.Dispose(); // Dispose the stream to reset the throttle manager

        await Task.Delay(3100); // Simulate a pause (cap refill)
        source = new MemoryStream();
        source.SetLength(1024 * 1024 * testSizeMB);
        target = new MemoryStream();
        throttledStream = new ThrottleEnabledStream(source, throttleManager); // Recreate the throttled stream

        start = DateTime.Now;
        await throttledStream.CopyToAsync(target);  // Copy again after pause
        elapsed = DateTime.Now - start; // Measure elapsed time again

        if (Math.Abs((elapsed - targetTime).TotalSeconds) > targetTime.TotalSeconds * delta)
            Assert.Fail($"Second elapsed time {elapsed} is not within {delta * 100}% of target time {targetTime}");

    }

    [Test]
    [TestCase(1000, 10, 0.05)]
    public async Task ThrottleStreamChange(int testSizeMB, int throttleMBs, double delta)
    {
        var source = new MemoryStream();
        source.SetLength(1024 * 1024 * testSizeMB);
        var target = new MemoryStream();

        var throttleManager = new ThrottleManager
        {
            Limit = 1024 * 1024 * throttleMBs
        };
        var throttledStream = new ThrottleEnabledStream(source, throttleManager);

        var targetTime1 = TimeSpan.FromSeconds(source.Length / throttleManager.Limit);

        var start = DateTime.Now;
        var copyTask = throttledStream.CopyToAsync(target);

        // Change throttle limit halfway through
        await Task.Delay((int)(targetTime1.TotalMilliseconds / 2));
        throttleManager.Limit = 1024 * 1024 * throttleMBs * 2;

        await copyTask;
        var elapsed = DateTime.Now - start;

        var targetTime2 = TimeSpan.FromSeconds(source.Length / throttleManager.Limit);
        var targetTime = targetTime1 / 2 + targetTime2 / 2;

        if (Math.Abs((elapsed - targetTime).TotalSeconds) > targetTime.TotalSeconds * delta)
            Assert.Fail($"Elapsed time {elapsed} is not within {delta * 100}% of target time {targetTime}");
    }

    [Test]
    [TestCase(500, 10, 0.05)]
    public async Task ThrottleTwoStreams_FairBandwidth(int testSizeMB, int throttleMBs, double delta)
    {
        var throttleManager = new ThrottleManager
        {
            Limit = 1024 * 1024 * throttleMBs
        };

        var source1 = new MemoryStream();
        var target1 = new MemoryStream();
        source1.SetLength(1024 * 1024 * testSizeMB / 2);

        var source2 = new MemoryStream();
        var target2 = new MemoryStream();
        source2.SetLength(1024 * 1024 * testSizeMB / 2);

        var throttledStream1 = new ThrottleEnabledStream(source1, throttleManager);
        var throttledStream2 = new ThrottleEnabledStream(source2, throttleManager);

        var start1 = DateTime.UtcNow;
        var start2 = DateTime.UtcNow;

        await Task.WhenAll(
            throttledStream1.CopyToAsync(target1),
            throttledStream2.CopyToAsync(target2)
        );

        var elapsed1 = DateTime.UtcNow - start1;
        var elapsed2 = DateTime.UtcNow - start2;

        var targetTimeSeconds = (source1.Length / (throttleManager.Limit / 2)); // Each stream gets half bandwidth

        Assert.That(Math.Abs(elapsed1.TotalSeconds - targetTimeSeconds) <= targetTimeSeconds * delta,
            $"Stream1 elapsed {elapsed1}, target {TimeSpan.FromSeconds(targetTimeSeconds)}");

        Assert.That(Math.Abs(elapsed2.TotalSeconds - targetTimeSeconds) <= targetTimeSeconds * delta,
            $"Stream2 elapsed {elapsed2}, target {TimeSpan.FromSeconds(targetTimeSeconds)}");
    }

    [Test]
    [TestCase(50, 5, 80 * 1024, 0.15)]
    [TestCase(50, 5, 64 * 1024, 0.15)]
    [TestCase(100, 5, 1 * 1024 * 1024, 0.25)]
    public async Task TransferSpeed_ShouldBeStableOverTime(int sizeMB, int throttleMBps, int bufferSize, double allowedVariation)
    {
        var throttleManager = new ThrottleManager
        {
            Limit = throttleMBps * 1024 * 1024
        };

        var source = new MemoryStream();
        source.SetLength(sizeMB * 1024L * 1024);
        var target = new MemoryStream();
        var throttledStream = new ThrottleEnabledStream(source, throttleManager);

        // Try to make the manager allocate some tokens before we start measuring
        await Task.Delay(2000);

        var speeds = new List<double>();
        var buffer = new byte[bufferSize];
        var stopwatch = Stopwatch.StartNew();
        var interval = TimeSpan.FromMilliseconds(510);
        var lastCheckpoint = stopwatch.Elapsed;
        long lastBytes = 0;

        var cts = new CancellationTokenSource();

        var monitorTask = Task.Run(async () =>
        {
            while (!cts.Token.IsCancellationRequested)
            {
                await Task.Delay(interval, cts.Token);

                var currentBytes = target.Length;
                var elapsed = stopwatch.Elapsed - lastCheckpoint;
                var bytesTransferred = currentBytes - lastBytes;

                var speed = bytesTransferred / elapsed.TotalSeconds / (1024 * 1024); // MB/s
                speeds.Add(speed);

                lastCheckpoint = stopwatch.Elapsed;
                lastBytes = currentBytes;
            }
        }, cts.Token);

        await throttledStream.CopyToAsync(target, bufferSize);

        stopwatch.Stop();
        cts.Cancel();

        try { await monitorTask; } catch (OperationCanceledException) { }

        Assert.That(target.Length, Is.EqualTo(source.Length), "Not all bytes were written.");
        Assert.That(target.Position, Is.EqualTo(source.Length), "Not all bytes were written.");

        Assert.That(speeds.Count, Is.GreaterThan(0), "No speed measurements were taken.");

        var min = speeds.Min();
        var max = speeds.Max();
        var average = speeds.Sum() / speeds.Count;

        Assert.That(min, Is.GreaterThanOrEqualTo(average * (1 - allowedVariation)),
            $"Minimum speed {min:F2} MB/s is below allowed variation from average {average:F2} MB/s");

        Assert.That(max, Is.LessThanOrEqualTo(average * (1 + allowedVariation)),
            $"Maximum speed {max:F2} MB/s is above allowed variation from average {average:F2} MB/s");
    }
}