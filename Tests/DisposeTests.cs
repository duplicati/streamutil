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

public class DisposeTests
{
    /// <summary>
    /// The ways a wrapping stream can be disposed
    /// </summary>
    public enum DisposeMethod
    {
        Dispose,
        Close,
        DisposeAsync,
        Using,
        AwaitUsing
    }

    /// <summary>
    /// A base stream that counts how many times it is disposed
    /// </summary>
    private sealed class CountingStream : MemoryStream
    {
        public int Disposals { get; private set; }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
                Disposals++;
            base.Dispose(disposing);
        }
    }

    /// <summary>
    /// A wrapping stream that counts how many times its own cleanup runs
    /// </summary>
    private sealed class CountingWrapper : WrappingStream
    {
        public int Cleanups { get; private set; }

        public CountingWrapper(Stream stream, bool disposeBaseStream)
            : base(stream, disposeBaseStream) { }

        public override int Read(byte[] buffer, int offset, int count) => BaseStream.Read(buffer, offset, count);
        public override void Write(byte[] buffer, int offset, int count) => BaseStream.Write(buffer, offset, count);

        protected override void Dispose(bool disposing)
        {
            if (disposing)
                Cleanups++;
            base.Dispose(disposing);
        }
    }

    /// <summary>
    /// An asynchronous wrapping stream that counts how many times its own cleanup runs
    /// </summary>
    private sealed class CountingAsyncWrapper : WrappingAsyncStream
    {
        public int Cleanups { get; private set; }

        public CountingAsyncWrapper(Stream stream, bool disposeBaseStream)
            : base(stream, disposeBaseStream) { }

        protected override Task<int> ReadImplAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
            => BaseStream.ReadAsync(buffer, offset, count, cancellationToken);

        protected override Task WriteImplAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
            => BaseStream.WriteAsync(buffer, offset, count, cancellationToken);

        protected override void Dispose(bool disposing)
        {
            if (disposing)
                Cleanups++;
            base.Dispose(disposing);
        }
    }

    private static async Task DisposeWith(Stream stream, DisposeMethod method)
    {
        switch (method)
        {
            case DisposeMethod.Dispose:
                stream.Dispose();
                break;
            case DisposeMethod.Close:
                stream.Close();
                break;
            case DisposeMethod.DisposeAsync:
                await stream.DisposeAsync();
                break;
            case DisposeMethod.Using:
                using (stream) { }
                break;
            case DisposeMethod.AwaitUsing:
                await using (stream) { }
                break;
        }
    }

    private static void AssertBaseStream(CountingStream baseStream, bool disposeBaseStream)
    {
        if (disposeBaseStream)
        {
            Assert.That(baseStream.Disposals, Is.EqualTo(1), "The base stream was not disposed exactly once");
        }
        else
        {
            Assert.That(baseStream.Disposals, Is.EqualTo(0), "The base stream was disposed");
            Assert.That(baseStream.CanRead, Is.True, "The base stream was closed");
        }
    }

    [Test]
    public async Task TimeoutObservingStreamDisposesTheBaseStreamOnlyWhenAsked(
        [Values] DisposeMethod method, [Values] bool disposeBaseStream)
    {
        var baseStream = new CountingStream();
        var stream = new TimeoutObservingStream(baseStream, disposeBaseStream) { ReadTimeout = 1000, WriteTimeout = 1000 };

        await DisposeWith(stream, method);

        AssertBaseStream(baseStream, disposeBaseStream);
    }

    [Test]
    public async Task ThrottleEnabledStreamDisposesTheBaseStreamOnce([Values] DisposeMethod method)
    {
        var baseStream = new CountingStream();
        var throttleManager = new ThrottleManager();
        var stream = new ThrottleEnabledStream(baseStream, throttleManager);

        await DisposeWith(stream, method);

        AssertBaseStream(baseStream, true);
    }

    [Test]
    public async Task WrappingStreamCleansUpOnceAndDisposesTheBaseStreamOnlyWhenAsked(
        [Values] DisposeMethod method, [Values] bool disposeBaseStream)
    {
        var baseStream = new CountingStream();
        var stream = new CountingWrapper(baseStream, disposeBaseStream);

        await DisposeWith(stream, method);

        Assert.That(stream.Cleanups, Is.EqualTo(1), "The wrapping stream did not clean up exactly once");
        AssertBaseStream(baseStream, disposeBaseStream);
    }

    [Test]
    public async Task WrappingAsyncStreamCleansUpOnceAndDisposesTheBaseStreamOnlyWhenAsked(
        [Values] DisposeMethod method, [Values] bool disposeBaseStream)
    {
        var baseStream = new CountingStream();
        var stream = new CountingAsyncWrapper(baseStream, disposeBaseStream);

        await DisposeWith(stream, method);

        Assert.That(stream.Cleanups, Is.EqualTo(1), "The wrapping stream did not clean up exactly once");
        AssertBaseStream(baseStream, disposeBaseStream);
    }
}
