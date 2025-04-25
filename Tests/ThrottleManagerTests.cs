
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

namespace Tests;

public class ThrottleManagerTests
{
    [Test]
    public void RegisterTransfer_AssignsUniqueIds()
    {
        using var manager = new ThrottleManager { Limit = 1000 };
        var id1 = manager.RegisterTransfer();
        var id2 = manager.RegisterTransfer();

        Assert.That(id1, Is.Not.EqualTo(id2));
    }

    [Test]
    public void UnregisterTransfer_DoesNotThrow()
    {
        using var manager = new ThrottleManager { Limit = 1000 };
        var id = manager.RegisterTransfer();
        manager.UnregisterTransfer(id);
        manager.UnregisterTransfer(id);
        manager.UnregisterTransfer(2001);
    }

    [Test]
    public void SleepForSize_RespectsLimit()
    {
        using var manager = new ThrottleManager { Limit = 1000 }; // 1 KB/sec
        var id = manager.RegisterTransfer();
        var start = DateTime.UtcNow;

        manager.SleepForSize(id, 2000); // Request 2KB

        var elapsed = (DateTime.UtcNow - start).TotalMilliseconds;
        Assert.True(elapsed >= 1000, "Should delay at least 1 second for 2KB at 1KB/s limit");
    }

    [Test]
    public async Task WaitForSize_AsyncDelayWorks()
    {
        using var manager = new ThrottleManager { Limit = 500 }; // 500 B/s
        var id = manager.RegisterTransfer();

        var start = DateTime.UtcNow;
        await manager.WaitForSize(id, 1000, CancellationToken.None); // Request 1KB

        var elapsed = (DateTime.UtcNow - start).TotalMilliseconds;
        Assert.That(elapsed, Is.AtLeast(1900), "Async delay should wait at least 2 seconds");
    }

    [Test]
    public void Dispose_PreventsFurtherUse()
    {
        var manager = new ThrottleManager { Limit = 1000 };
        manager.Dispose();
        Assert.Throws<ObjectDisposedException>(() => manager.RegisterTransfer());
        Assert.Throws<ObjectDisposedException>(() => manager.SleepForSize(1, 100));
    }
}
