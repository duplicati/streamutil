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

public class MeasureTest
{
    [Test]
    [TestCase(1000, 10, 0.02, 0.25)]
    public async Task MeasureThrottledStream(int testSizeMB, int throttleMBs, double delta, double deltaForWindow)
    {
        var source = new MemoryStream();
        source.SetLength(1024 * 1024 * testSizeMB);
        var target = new MemoryStream();

        var throttleManager = new ThrottleManager
        {
            Limit = 1024 * 1024 * throttleMBs
        };
        var throttledStream = new ThrottleEnabledStream(source, throttleManager);
        var measureStream = new SpeedMeasuringStream(throttledStream);

        // Use Stopwatch for more precise timing
        var stopwatch = Stopwatch.StartNew();
        var copyTask = measureStream.CopyToAsync(target);

        // Wait a bit longer to let the throttling stabilize
        var expectedDuration = TimeSpan.FromSeconds(testSizeMB / throttleMBs);
        await Task.Delay(expectedDuration / 4); // Wait for 25% of expected duration

        var totalSpeed1 = measureStream.TotalBytesPerSecond;
        var totalWindow1 = measureStream.RecentBytesPerSecond;

        await copyTask;
        stopwatch.Stop();

        var totalSpeed2 = measureStream.TotalBytesPerSecond;
        var totalWindow2 = measureStream.RecentBytesPerSecond;

        Console.WriteLine($"Total Speed 1: {totalSpeed1:N0} bytes/sec");
        Console.WriteLine($"Total Speed 2: {totalSpeed2:N0} bytes/sec");
        Console.WriteLine($"Window Speed 1: {totalWindow1:N0} bytes/sec");
        Console.WriteLine($"Window Speed 2: {totalWindow2:N0} bytes/sec");
        Console.WriteLine($"Throttle Limit: {throttleManager.Limit:N0} bytes/sec");
        Console.WriteLine($"Elapsed Time: {stopwatch.Elapsed.TotalSeconds:N2} seconds");

        // Total speed should be very accurate
        Assert.That(Math.Abs(totalSpeed1 - throttleManager.Limit) / throttleManager.Limit, Is.LessThan(delta));
        Assert.That(Math.Abs(totalSpeed2 - throttleManager.Limit) / throttleManager.Limit, Is.LessThan(delta));

        // Window speed can have more variation because its a sliding window
        Assert.That(Math.Abs(totalWindow1 - throttleManager.Limit) / throttleManager.Limit,
            Is.LessThan(deltaForWindow));
        Assert.That(Math.Abs(totalWindow2 - throttleManager.Limit) / throttleManager.Limit,
            Is.LessThan(deltaForWindow));
    }
}