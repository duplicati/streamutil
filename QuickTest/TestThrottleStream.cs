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

namespace Duplicati.StreamUtil.QuickTest;

public static class TestThrottleStream
{
    public static async Task Run()
    {
        var source = new MemoryStream();
        source.SetLength(1024 * 1024 * 100);
        var target = new MemoryStream();

        var throttleManager = new ThrottleManager
        {
            Limit = 1024 * 1024 * 10 // 10 MB/s
        };
        var throttledStream = new ThrottleEnabledStream(source, throttleManager);

        var start = DateTime.Now;
        await throttledStream.CopyToAsync(target);
        var elapsed = DateTime.Now - start;
        var speed = source.Length / elapsed.TotalSeconds;
        if (speed > 1024 * 1024)
            Console.WriteLine($"Speed: {speed / 1024 / 1024:F2} MB/s - {elapsed}");
        else
            Console.WriteLine($"Speed: {speed / 1024:F2} KB/s - {elapsed}");
        
        Console.WriteLine($"Expected: {throttleManager.Limit / 1024 /1024} MB/s");
    }
}
