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

namespace Duplicati.StreamUtil;

/// <summary>
/// A stream that wraps another stream and throttles read and write operations.
/// </summary>
public sealed class ThrottleEnabledStream : WrappingStream
{
    /// <summary>
    /// The throttle manager that determines how long to delay read operations.
    /// </summary>
    public ThrottleManager ReadThrottleManager { get; }

    /// <summary>
    /// The throttle manager that determines how long to delay write operations.
    /// </summary>
    public ThrottleManager WriteThrottleManager { get; }

    /// <summary>
    /// Creates a new ThrottleEnabledStream.
    /// </summary>
    /// <param name="baseStream">The stream to wrap.</param>
    /// <param name="readThrottleManager">The throttle manager to use for reads.</param>
    /// <param name="writeThrottleManager">The throttle manager to use for writes.</param>
    public ThrottleEnabledStream(Stream baseStream, ThrottleManager readThrottleManager, ThrottleManager writeThrottleManager)
        : base(baseStream)
    {
        ReadThrottleManager = readThrottleManager;
        WriteThrottleManager = writeThrottleManager;
    }

    /// <summary>
    /// Creates a new ThrottleEnabledStream.
    /// </summary>
    /// <param name="baseStream">The stream to wrap.</param>
    /// <param name="throttleManager">The throttle manager to use for both reads and writes.</param>
    public ThrottleEnabledStream(Stream baseStream, ThrottleManager throttleManager)
        : this(baseStream, throttleManager, throttleManager) { }

    /// <summary>
    /// Creates a new ThrottleEnabledStream.
    /// </summary>
    /// <param name="baseStream">The stream to wrap.</param>
    /// <param name="readThrottle">The throttle limit for reads in bytes/s.</param>
    /// <param name="writeThrottle">The throttle limit for writes in bytes/s.</param>
    public ThrottleEnabledStream(Stream baseStream, int readThrottle, int writeThrottle)
        : this(baseStream, new ThrottleManager() { Limit = readThrottle }, new ThrottleManager() { Limit = writeThrottle }) { }

    /// <inheritdoc />
    public override int Read(byte[] buffer, int offset, int count)
    {
        int bytesRead = BaseStream.Read(buffer, offset, count);
        ReadThrottleManager.SleepForSize(bytesRead);
        return bytesRead;
    }

    /// <inheritdoc />
    public override void Write(byte[] buffer, int offset, int count)
    {
        BaseStream.Write(buffer, offset, count);
        WriteThrottleManager.SleepForSize(count);

    }

    /// <inheritdoc />
    public override async Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
        int bytesRead = await BaseStream.ReadAsync(buffer, offset, count, cancellationToken);
        await ReadThrottleManager.WaitForSize(bytesRead, cancellationToken);
        return bytesRead;
    }

    /// <inheritdoc />
    public override async Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
        await BaseStream.WriteAsync(buffer, offset, count, cancellationToken);
        await WriteThrottleManager.WaitForSize(count, cancellationToken);
    }
}
