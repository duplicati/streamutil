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

namespace Tests;

public class ThrottleTests
{
    [Test]
    [TestCase(1000,10,0.05)]
    public async Task ThrottleStream(int testSizeMB, int throttleMBs, double delta)
    {
        var source = new MemoryStream();
        source.SetLength(1024 * 1024 * testSizeMB);
        var target = new MemoryStream();

        var throttleManager = new StreamUtil.ThrottleManager
        {
            Limit = 1024 * 1024 * throttleMBs
        };
        var throttledStream = new StreamUtil.ThrottleEnabledStream(source, throttleManager);

        var start = DateTime.Now;
        await throttledStream.CopyToAsync(target);
        var elapsed = DateTime.Now - start;

        var targetTime = TimeSpan.FromSeconds(source.Length / throttleManager.Limit);

        if (Math.Abs((elapsed - targetTime).TotalSeconds) > targetTime.TotalSeconds * delta)
            Assert.Fail($"Elapsed time {elapsed} is not within {delta * 100}% of target time {targetTime}");
    }

    [Test]
    [TestCase(1000,10,0.05)]
    public async Task ThrottleStreamChange(int testSizeMB, int throttleMBs, double delta)
    {
        var source = new MemoryStream();
        source.SetLength(1024 * 1024 * testSizeMB);
        var target = new MemoryStream();

        var throttleManager = new StreamUtil.ThrottleManager
        {
            Limit = 1024 * 1024 * throttleMBs
        };
        var throttledStream = new StreamUtil.ThrottleEnabledStream(source, throttleManager);

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
}