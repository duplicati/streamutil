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

using System.IO.Pipelines;
using Duplicati.StreamUtil;

namespace Tests;

public class DisposeTests
{
    [Test]
    [TestCase(false)]
    [TestCase(true)]
    public void DisposeRespectsDisposeBaseStream(bool disposeBaseStream)
    {
        var baseStream = new MemoryStream();
        new TimeoutObservingStream(baseStream, disposeBaseStream).Dispose();
        Assert.That(baseStream.CanRead, Is.EqualTo(!disposeBaseStream));
    }

    [Test]
    [TestCase(false)]
    [TestCase(true)]
    public void CloseRespectsDisposeBaseStream(bool disposeBaseStream)
    {
        var baseStream = new MemoryStream();
        new TimeoutObservingStream(baseStream, disposeBaseStream).Close();
        Assert.That(baseStream.CanRead, Is.EqualTo(!disposeBaseStream));
    }

    [Test]
    [TestCase(false)]
    [TestCase(true)]
    public async Task DisposeAsyncRespectsDisposeBaseStream(bool disposeBaseStream)
    {
        var baseStream = new MemoryStream();
        await new TimeoutObservingStream(baseStream, disposeBaseStream).DisposeAsync();
        Assert.That(baseStream.CanRead, Is.EqualTo(!disposeBaseStream));
    }

    [Test]
    public async Task DisposeAsyncDisposesBaseStreamOnce()
    {
        var baseStream = new CountingStream();
        await new TimeoutObservingStream(baseStream, true).DisposeAsync();
        Assert.That(baseStream.DisposeCount, Is.EqualTo(1));
    }

    [Test]
    public async Task DisposeAsyncRunsWrapperCleanupBeforeDisposingBaseStream()
    {
        var baseStream = new CountingStream();
        var wrapper = new CleanupRecordingStream(baseStream);
        await wrapper.DisposeAsync();

        Assert.That(wrapper.BaseStreamWasOpenDuringCleanup, Is.True);
        Assert.That(baseStream.DisposeCount, Is.EqualTo(1));
    }

    [Test]
    public async Task WriteTimeoutDoesNotCompletePipeBeforeTheErrorIsReported()
    {
        var pipe = new Pipe();
        var chunk = new byte[1024 * 1024];

        // Mirrors a download into a pipe: the wrapper is disposed while the timeout propagates,
        // and only then is the pipe completed with the error
        var writer = Task.Run(async () =>
        {
            try
            {
                using var wrapper = new TimeoutObservingStream(pipe.Writer.AsStream(), false)
                {
                    WriteTimeout = (int)TimeSpan.FromSeconds(1).TotalMilliseconds
                };

                for (var i = 0; i < 20; i++)
                    await wrapper.WriteAsync(chunk, 0, chunk.Length, CancellationToken.None);
            }
            catch (Exception ex)
            {
                await pipe.Writer.CompleteAsync(ex);
            }
            finally
            {
                await pipe.Writer.CompleteAsync();
            }
        });

        // The reader stalls longer than the write timeout, then reads the rest
        var reader = pipe.Reader.AsStream();
        await Task.Delay(TimeSpan.FromSeconds(2));
        Assert.ThrowsAsync<TimeoutException>(async () =>
        {
            while (await reader.ReadAsync(chunk, 0, chunk.Length) > 0) { }
        });

        await writer;
    }

    /// <summary>
    /// A stream that counts how many times it is disposed
    /// </summary>
    private sealed class CountingStream : MemoryStream
    {
        /// <summary>
        /// The number of times the stream was disposed
        /// </summary>
        public int DisposeCount { get; private set; }

        /// <summary>
        /// Gets a value indicating whether the stream has not been disposed
        /// </summary>
        public bool IsOpen => DisposeCount == 0;

        /// <inheritdoc/>
        protected override void Dispose(bool disposing)
        {
            if (disposing)
                DisposeCount++;
            base.Dispose(disposing);
        }
    }

    /// <summary>
    /// A wrapper that records whether the base stream was still open when its own cleanup ran
    /// </summary>
    private sealed class CleanupRecordingStream(CountingStream stream) : WrappingAsyncStream(stream, true)
    {
        /// <summary>
        /// Gets a value indicating whether the base stream was open when the cleanup ran
        /// </summary>
        public bool BaseStreamWasOpenDuringCleanup { get; private set; }

        /// <inheritdoc/>
        protected override Task<int> ReadImplAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
            => BaseStream.ReadAsync(buffer, offset, count, cancellationToken);

        /// <inheritdoc/>
        protected override Task WriteImplAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
            => BaseStream.WriteAsync(buffer, offset, count, cancellationToken);

        /// <inheritdoc/>
        protected override void Dispose(bool disposing)
        {
            if (disposing)
                BaseStreamWasOpenDuringCleanup = stream.IsOpen;
            base.Dispose(disposing);
        }
    }
}
