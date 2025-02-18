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
/// Class that wraps a stream and allows for intercepting read and write operations.
/// Intended for use in testing.
/// </summary>
public class LambdaInterceptStream : WrappingStream
{
    /// <summary>
    /// Initializes a new instance of the <see cref="LambdaInterceptStream"/> class.
    /// </summary>
    /// <param name="baseStream">The stream to wrap.</param>
    public LambdaInterceptStream(Stream baseStream) : base(baseStream) { }

    /// <summary>
    /// The action to perform before a read operation.
    /// </summary>
    public Action<byte[], int, int>? PreReadTask { get; set; }
    /// <summary>
    /// The action to perform after a read operation.
    /// </summary>
    public Action<byte[], int, int, int>? PostReadTask { get; set; }
    /// <summary>
    /// The action to perform before a write operation.
    /// </summary>
    public Action<byte[], int, int>? PreWriteTask { get; set; }
    /// <summary>
    /// The action to perform after a write operation.
    /// </summary>
    public Action<byte[], int, int, int>? PostWriteTask { get; set; }

    /// <inheritdoc/>
    public override int Read(byte[] buffer, int offset, int count)
    {
        PreReadTask?.Invoke(buffer, offset, count);
        var read = BaseStream.Read(buffer, offset, count);
        PostReadTask?.Invoke(buffer, offset, count, read);
        return read;
    }

    /// <inheritdoc/>
    public override void Write(byte[] buffer, int offset, int count)
    {
        PreWriteTask?.Invoke(buffer, offset, count);
        BaseStream.Write(buffer, offset, count);
        PostWriteTask?.Invoke(buffer, offset, count, count);
    }
}
