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
    /// The transfer ID for the read operation.
    /// </summary>
    private readonly long readThrottleManagerTransferId;
    /// <summary>
    /// The transfer ID for the write operation.
    /// </summary>
    private readonly long writeThrottleManagerTransferId;

    /// <summary>
    /// Creates a new ThrottleEnabledStream.
    /// </summary>
    /// <param name="baseStream">The stream to wrap.</param>
    /// <param name="readThrottleManager">The throttle manager to use for reads.</param>
    /// <param name="writeThrottleManager">The throttle manager to use for writes.</param>
    public ThrottleEnabledStream(Stream baseStream, ThrottleManager readThrottleManager, ThrottleManager writeThrottleManager)
        : base(baseStream)
    {
        readThrottleManagerTransferId = readThrottleManager.RegisterTransfer();
        writeThrottleManagerTransferId = writeThrottleManager.RegisterTransfer();
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

    /// <summary>
    /// Calculates the chunk size for throttling based on the limit and count.
    /// </summary>
    /// <param name="limit">The throttle limit in bytes/s.</param>
    /// <param name="count">The number of bytes to read or write.</param>
    /// <returns>>The chunk size in bytes.</returns>
    private static int GetChunkSize(long limit, int count)
    {
        if (limit <= 100) // Avoid very small limits
            limit = int.MaxValue;
        else
            limit /= 10; // Limit is in bytes/s, and we use 100ms chunks

        return (int)Math.Min(count, limit);
    }

    /// <inheritdoc />
    public override int Read(byte[] buffer, int offset, int count)
    {
        var chunkSize = GetChunkSize(ReadThrottleManager.Limit, count);
        var bytesToRead = Math.Min(chunkSize, count);
        var bytesRead = BaseStream.Read(buffer, offset, bytesToRead);
        ReadThrottleManager.SleepForSize(readThrottleManagerTransferId, bytesRead);
        return bytesRead;
    }

    /// <inheritdoc />
    public override void Write(byte[] buffer, int offset, int count)
    {
        var chunkSize = GetChunkSize(WriteThrottleManager.Limit, count);
        while (count > 0)
        {
            var bytesToWrite = Math.Min(chunkSize, count);
            WriteThrottleManager.SleepForSize(writeThrottleManagerTransferId, bytesToWrite);
            BaseStream.Write(buffer, offset, bytesToWrite);
            offset += bytesToWrite;
            count -= bytesToWrite;
        }
    }

    /// <inheritdoc />
    public override async Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
        var chunkSize = GetChunkSize(ReadThrottleManager.Limit, count);
        var bytesToRead = Math.Min(chunkSize, count);
        var bytesRead = await BaseStream.ReadAsync(buffer, offset, bytesToRead, cancellationToken);
        await ReadThrottleManager.WaitForSize(readThrottleManagerTransferId, bytesRead, cancellationToken);
        return bytesRead;
    }

    /// <inheritdoc />
    public override async Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
        var chunkSize = GetChunkSize(WriteThrottleManager.Limit, count);
        while (count > 0)
        {
            var bytesToWrite = Math.Min(chunkSize, count);
            await WriteThrottleManager.WaitForSize(writeThrottleManagerTransferId, bytesToWrite, cancellationToken);
            await BaseStream.WriteAsync(buffer, offset, bytesToWrite, cancellationToken);
            await BaseStream.FlushAsync(cancellationToken);
            offset += bytesToWrite;
            count -= bytesToWrite;
        }
    }

    /// <inheritdoc />
    protected override void Dispose(bool disposing)
    {
        ReadThrottleManager.UnregisterTransfer(readThrottleManagerTransferId);
        WriteThrottleManager.UnregisterTransfer(writeThrottleManagerTransferId);
        base.Dispose(disposing);
    }

    /// <inheritdoc />
    public override async ValueTask DisposeAsync()
    {
        ReadThrottleManager.UnregisterTransfer(readThrottleManagerTransferId);
        WriteThrottleManager.UnregisterTransfer(writeThrottleManagerTransferId);
        await base.DisposeAsync();
    }
}
