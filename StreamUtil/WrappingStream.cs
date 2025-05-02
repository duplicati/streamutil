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
/// Wraps a <see cref="Stream"/> and delegates all calls to it.
/// </summary>
public abstract class WrappingStream : Stream
{
    /// <summary>
    /// The stream being wrapped.
    /// </summary>
    public Stream BaseStream { get; init; }

    /// <summary>
    /// Gets or sets a value indicating whether the base stream should be disposed when this stream is disposed.
    /// </summary>
    public bool DisposeBaseStream { get; set; }

    /// <summary>
    /// Initializes a new instance of the <see cref="WrappingStream"/> class.
    /// </summary>
    /// <param name="stream">The stream to wrap.</param>
    protected WrappingStream(Stream stream)
        : this(stream, true)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="WrappingStream"/> class.
    /// </summary>
    /// <param name="stream">The stream to wrap.</param>
    /// <param name="disposeBaseStream">Whether to dispose the base stream when this stream is disposed.</param>
    protected WrappingStream(Stream stream, bool disposeBaseStream)
    {
        BaseStream = stream;
        DisposeBaseStream = disposeBaseStream;
    }

    /// <inheritdoc/>
    public override bool CanTimeout => BaseStream.CanTimeout;
    /// <inheritdoc/>
    public override bool CanRead => BaseStream.CanRead;
    /// <inheritdoc/>
    public override bool CanSeek => BaseStream.CanSeek;
    /// <inheritdoc/>
    public override bool CanWrite => BaseStream.CanWrite;
    /// <inheritdoc/>
    public override long Length => BaseStream.Length;
    /// <inheritdoc/>
    public override long Position
    {
        get => BaseStream.Position;
        set => BaseStream.Position = value;
    }

    /// <inheritdoc/>
    public override void Flush() => BaseStream.Flush();
    /// <inheritdoc/>
    public override long Seek(long offset, SeekOrigin origin) => BaseStream.Seek(offset, origin);
    /// <inheritdoc/>
    public override void SetLength(long value) => BaseStream.SetLength(value);

    /// <inheritdoc/>
    protected override void Dispose(bool disposing)
    {
        if (disposing && DisposeBaseStream)
            BaseStream.Dispose();
    }

    /// <inheritdoc/>
    public override async ValueTask DisposeAsync()
    {
        if (DisposeBaseStream)
            await BaseStream.DisposeAsync();

        // Also dispose self if needed
        await base.DisposeAsync();
    }

    /// <inheritdoc/>
    public override void Close()
    {
        Dispose(true);
        BaseStream.Close();
    }

    /// <inheritdoc/>
    public override Task FlushAsync(CancellationToken cancellationToken)
        => BaseStream.FlushAsync(cancellationToken);
}
