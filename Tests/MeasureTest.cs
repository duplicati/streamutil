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

using System.Diagnostics;

namespace Tests;

public class MeasureTest
{
    [Test]
    public async Task MeasureThrottledStream()
    {
        var delta = 0.01;
        var source = new MemoryStream();
        source.SetLength(1024 * 1024 * 100); // 100 MB
        var target = new MemoryStream();

        var throttleManager = new StreamUtil.ThrottleManager
        {
            Limit = 1024 * 1024 * 10 // 10 MB/s
        };
        var throttledStream = new StreamUtil.ThrottleEnabledStream(source, throttleManager);
        var measureStream = new StreamUtil.SpeedMeasuringStream(throttledStream);

        // Use Stopwatch for more precise timing
        var stopwatch = Stopwatch.StartNew();
        var copyTask = measureStream.CopyToAsync(target);
    
        // Wait a bit longer to let the throttling stabilize
        await Task.Delay(TimeSpan.FromSeconds(5));
    
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

        Assert.That(Math.Abs(totalSpeed1 - throttleManager.Limit), Is.LessThan(throttleManager.Limit * delta));
        Assert.That(Math.Abs(totalSpeed2 - throttleManager.Limit), Is.LessThan(throttleManager.Limit * delta));
        Assert.That(Math.Abs(totalWindow1 - throttleManager.Limit), Is.LessThan(throttleManager.Limit * delta));
        Assert.That(Math.Abs(totalWindow2 - throttleManager.Limit), Is.LessThan(throttleManager.Limit * delta));
    }
}
