
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

using Duplicati.StreamUtil;
using System.Diagnostics;

namespace Tests;

public class ThrottleManagerFairnessTests
{
    private const int TestLimit = 1024 * 10; // 10 KB/s

    [Test]
    public async Task MultipleStreams_RunAtFullSpeedAndRespectLimits()
    {
        using var manager = new ThrottleManager { Limit = TestLimit };
        var ids = new List<long> { manager.RegisterTransfer(), manager.RegisterTransfer() };

        var tasks = new List<Task>();
        foreach (var id in ids)
        {
            tasks.Add(Task.Run(async () =>
            {
                for (int i = 0; i < 5; i++)
                {
                    await manager.WaitForSize(id, 2048, CancellationToken.None); // 2 KB per chunk
                }
            }));
        }

        var sw = Stopwatch.StartNew();
        await Task.WhenAll(tasks);
        sw.Stop();

        Assert.That(sw.Elapsed.TotalSeconds, Is.GreaterThanOrEqualTo(1), "Transfers should be throttled to fit the limit");
    }

    [Test]
    public async Task RefillCap_TriggeredAfterInactivity()
    {
        var manager = new ThrottleManager { Limit = 1024 }; // 1 KB/s
        var id = manager.RegisterTransfer();

        await manager.WaitForSize(id, 1024, CancellationToken.None); // request 4 KB

        await Task.Delay(3000); // idle for 3s to trigger refill

        var sw = Stopwatch.StartNew();
        await manager.WaitForSize(id, 4 * 1024, CancellationToken.None); // request 4 KB
        sw.Stop();

        Console.WriteLine($"Elapsed ms: {sw.Elapsed.TotalMilliseconds}");

        Assert.That(sw.Elapsed.TotalMilliseconds, Is.GreaterThanOrEqualTo(500),
            "Refill cap should limit token burst after idle");
        Assert.That(sw.Elapsed.TotalMilliseconds, Is.LessThanOrEqualTo(3000),
            "Refill cap should not have the first tokens available immediately after idle");
    }

    [Test]
    public async Task Fairness_OneFastOneSlowStream_ShouldBalance()
    {
        using var manager = new ThrottleManager { Limit = TestLimit };
        var fast = manager.RegisterTransfer();
        var slow = manager.RegisterTransfer();

        var fastTotal = 0L;
        var slowTotal = 0L;

        var fastTask = Task.Run(async () =>
        {
            for (int i = 0; i < 10; i++)
            {
                await manager.WaitForSize(fast, 1024, CancellationToken.None);
                Interlocked.Add(ref fastTotal, 1024);
            }
        });

        var slowTask = Task.Run(async () =>
        {
            for (int i = 0; i < 5; i++)
            {
                await Task.Delay(200); // simulate slowness
                await manager.WaitForSize(slow, 1024, CancellationToken.None);
                Interlocked.Add(ref slowTotal, 1024);
            }
        });

        await Task.WhenAll(fastTask, slowTask);

        var ratio = (double)fastTotal / slowTotal;
        Assert.That(ratio, Is.LessThan(3), "Faster stream should not exceed fair token share excessively");
    }
}
