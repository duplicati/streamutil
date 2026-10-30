// Copyright (C) 2026, The Duplicati Team
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

using System.IO.Pipelines;
using Duplicati.StreamUtil;

namespace Tests;

/// <summary>
/// The mode decides whether the time inside a call, the time between calls, or both count against the timeout.
/// Each timer covers one direction, so only the timeout for the direction in use is set.
/// </summary>
public class TimeoutModeTests
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan Longer = Timeout * 1.5;
    private static readonly TimeSpan Shorter = Timeout * 0.5;
    private const int Chunk = 1024 * 256;

    private static TimeoutObservingStream WrapForReading(Stream stream, TimeoutMode mode)
        => new(stream, false) { Mode = mode, ReadTimeout = (int)Timeout.TotalMilliseconds };

    private static TimeoutObservingStream WrapForWriting(Stream stream, TimeoutMode mode)
        => new(stream, false) { Mode = mode, WriteTimeout = (int)Timeout.TotalMilliseconds };

    /// <summary>
    /// A stream whose every read and write takes a fixed time to answer
    /// </summary>
    private sealed class SlowStream(TimeSpan delay) : Stream
    {
        public long Transferred;
        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => true;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
        public override void Flush() { }
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override int Read(byte[] buffer, int offset, int count) => ReadAsync(buffer, offset, count, default).GetAwaiter().GetResult();
        public override void Write(byte[] buffer, int offset, int count) => WriteAsync(buffer, offset, count, default).GetAwaiter().GetResult();

        public override async Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            await Task.Delay(delay, cancellationToken);
            Transferred += count;
            return count;
        }

        public override async Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            await Task.Delay(delay, cancellationToken);
            Transferred += count;
        }
    }

    // Time inside a call

    [Test]
    [TestCase(TimeoutMode.Call)]
    [TestCase(TimeoutMode.CallAndGap)]
    public void ASlowAnswerFailsWhenCallsCount(TimeoutMode mode)
    {
        using var wrapper = WrapForReading(new SlowStream(Longer), mode);
        Assert.ThrowsAsync<TimeoutException>(() => wrapper.ReadAsync(new byte[Chunk], 0, Chunk));
    }

    [Test]
    public async Task ASlowAnswerIsAcceptedWhenOnlyGapsCount()
    {
        var slow = new SlowStream(Longer);
        using var wrapper = WrapForWriting(slow, TimeoutMode.Gap);
        await wrapper.WriteAsync(new byte[Chunk], 0, Chunk);
        await wrapper.WriteAsync(new byte[Chunk], 0, Chunk);
        Assert.That(slow.Transferred, Is.EqualTo(2 * Chunk));
    }

    [Test]
    public async Task ADestinationHeldBackByItsConsumerIsAcceptedWhenOnlyGapsCount()
    {
        // The customer case: the writer is paused by backpressure while the consumer is busy elsewhere
        var pipe = new Pipe(new PipeOptions(pauseWriterThreshold: Chunk, resumeWriterThreshold: Chunk / 2));
        using var wrapper = WrapForWriting(pipe.Writer.AsStream(), TimeoutMode.Gap);

        var writer = Task.Run(async () =>
        {
            for (var i = 0; i < 8; i++)
                await wrapper.WriteAsync(new byte[Chunk], 0, Chunk);
            pipe.Writer.Complete();
        });

        var reader = Task.Run(async () =>
        {
            using var input = pipe.Reader.AsStream();
            var buffer = new byte[Chunk];
            long total = 0;
            // Take the first chunk, then leave the writer blocked for longer than the timeout
            total += await input.ReadAsync(buffer, 0, buffer.Length);
            await Task.Delay(Longer);
            while (true)
            {
                var read = await input.ReadAsync(buffer, 0, buffer.Length);
                if (read == 0)
                    return total;
                total += read;
            }
        });

        await writer;
        Assert.That(await reader, Is.EqualTo(8L * Chunk));
    }

    // Time between calls

    [Test]
    [TestCase(TimeoutMode.Gap)]
    [TestCase(TimeoutMode.CallAndGap)]
    public async Task ALongGapFailsWhenGapsCount(TimeoutMode mode)
    {
        using var wrapper = WrapForReading(new SlowStream(TimeSpan.Zero), mode);
        await wrapper.ReadAsync(new byte[Chunk], 0, Chunk);
        await Task.Delay(Longer);
        Assert.ThrowsAsync<TimeoutException>(() => wrapper.ReadAsync(new byte[Chunk], 0, Chunk));
    }

    [Test]
    public async Task ALongGapIsAcceptedWhenOnlyCallsCount()
    {
        var fast = new SlowStream(TimeSpan.Zero);
        using var wrapper = WrapForReading(fast, TimeoutMode.Call);
        await wrapper.ReadAsync(new byte[Chunk], 0, Chunk);
        await Task.Delay(Longer);
        await wrapper.ReadAsync(new byte[Chunk], 0, Chunk);
        Assert.That(fast.Transferred, Is.EqualTo(2 * Chunk));
    }

    [Test]
    [TestCase(TimeoutMode.Gap)]
    [TestCase(TimeoutMode.CallAndGap)]
    public async Task ALongWaitBeforeTheFirstCallFailsWhenGapsCount(TimeoutMode mode)
    {
        using var wrapper = WrapForWriting(new SlowStream(TimeSpan.Zero), mode);
        await Task.Delay(Longer);
        Assert.ThrowsAsync<TimeoutException>(() => wrapper.WriteAsync(new byte[Chunk], 0, Chunk));
    }

    [Test]
    public async Task ALongWaitBeforeTheFirstCallIsAcceptedWhenOnlyCallsCount()
    {
        var fast = new SlowStream(TimeSpan.Zero);
        using var wrapper = WrapForWriting(fast, TimeoutMode.Call);
        await Task.Delay(Longer);
        await wrapper.WriteAsync(new byte[Chunk], 0, Chunk);
        Assert.That(fast.Transferred, Is.EqualTo(Chunk));
    }

    // Within the timeout, every mode accepts the transfer

    [Test]
    [TestCase(TimeoutMode.Call)]
    [TestCase(TimeoutMode.Gap)]
    [TestCase(TimeoutMode.CallAndGap)]
    public async Task ShortAnswersAndShortGapsAreAcceptedInEveryMode(TimeoutMode mode)
    {
        var slow = new SlowStream(Shorter / 2);
        using var wrapper = WrapForReading(slow, mode);
        for (var i = 0; i < 3; i++)
        {
            await wrapper.ReadAsync(new byte[Chunk], 0, Chunk);
            await Task.Delay(Shorter / 2);
        }
        Assert.That(slow.Transferred, Is.EqualTo(3 * Chunk));
    }

    // A timeout is permanent

    [Test]
    public async Task ATimeoutInAGapFailsEveryLaterCall()
    {
        var fast = new SlowStream(TimeSpan.Zero);
        using var wrapper = WrapForReading(fast, TimeoutMode.Gap);
        await Task.Delay(Longer);
        Assert.ThrowsAsync<TimeoutException>(() => wrapper.ReadAsync(new byte[Chunk], 0, Chunk));
        Assert.ThrowsAsync<TimeoutException>(() => wrapper.ReadAsync(new byte[Chunk], 0, Chunk));
        Assert.That(fast.Transferred, Is.EqualTo(0));
    }

    // The mode can be chosen after the timeouts

    [Test]
    public async Task SwitchingToCallModeStopsTheTimerBetweenCalls()
    {
        var fast = new SlowStream(TimeSpan.Zero);
        using var wrapper = new TimeoutObservingStream(fast, false) { ReadTimeout = (int)Timeout.TotalMilliseconds };
        wrapper.Mode = TimeoutMode.Call;
        await Task.Delay(Longer);
        await wrapper.ReadAsync(new byte[Chunk], 0, Chunk);
        Assert.That(fast.Transferred, Is.EqualTo(Chunk));
    }
}
