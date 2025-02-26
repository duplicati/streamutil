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

using System.IO.Pipelines;
using Duplicati.StreamUtil;

namespace Tests;

public class TimeoutTests
{
    [Test]
    [TestCase(1)]
    [TestCase(3)]
    [TestCase(5)]
    public async Task WritePauseLessThanTimeoutShouldComplete(int timeoutSeconds)
    {
        var pipe = new Pipe();

        var output = pipe.Writer.AsStream();
        var input = pipe.Reader.AsStream();

        var bytes = 1024 * 1024 * 2;
        var wrapper = new TimeoutObservingStream(input)
        {
            ReadTimeout = (int)TimeSpan.FromSeconds(timeoutSeconds).TotalMilliseconds
        };

        var writer = WritePauseWrite(output, bytes / 2, TimeSpan.FromSeconds(timeoutSeconds - 0.5), bytes / 2);
        var reader = ReadAll(wrapper, bytes);

        var result = await reader;
        Assert.That(result, Is.EqualTo(bytes));
    }

    [Test]
    [TestCase(1)]
    [TestCase(3)]
    [TestCase(5)]
    public async Task ReadPauseLessThanTimeoutShouldComplete(int timeoutSeconds)
    {
        var pipe = new Pipe();

        var output = pipe.Writer.AsStream();
        var input = pipe.Reader.AsStream();

        var bytes = 1024 * 1024 * 2;
        var wrapper = new TimeoutObservingStream(output)
        {
            WriteTimeout = (int)TimeSpan.FromSeconds(timeoutSeconds).TotalMilliseconds
        };

        var writer = WriteAll(wrapper, bytes);
        var reader = ReadPauseRead(input, bytes / 2, TimeSpan.FromSeconds(timeoutSeconds - 0.5), bytes / 2);

        await writer;
        Assert.That(reader.Result, Is.EqualTo(bytes));
    }

    [Test]
    [TestCase(1)]
    [TestCase(3)]
    [TestCase(5)]
    public async Task WritePauseMoreThanTimeoutShouldThrow(int timeoutSeconds)
    {
        var pipe = new Pipe();

        var output = pipe.Writer.AsStream();
        var input = pipe.Reader.AsStream();

        var bytes = 1024 * 1024 * 2;
        var wrapper = new TimeoutObservingStream(input)
        {
            ReadTimeout = (int)TimeSpan.FromSeconds(timeoutSeconds).TotalMilliseconds
        };

        var writer = WritePauseWrite(output, bytes / 2, TimeSpan.FromSeconds(timeoutSeconds + 0.5), bytes / 2);
        var reader = ReadAll(wrapper, bytes);

        try
        {
            await reader;
        }
        catch (TimeoutException)
        {
            // Expected
            return;
        }

        Assert.Fail("TimeoutException not thrown");
    }

    [Test]
    [TestCase(1)]
    [TestCase(3)]
    [TestCase(5)]
    public async Task ReadPauseMoreThanTimeoutShouldThrow(int timeoutSeconds)
    {
        var pipe = new Pipe();

        var output = pipe.Writer.AsStream();
        var input = pipe.Reader.AsStream();

        var bytes = 1024 * 1024 * 2;
        var wrapper = new TimeoutObservingStream(output)
        {
            WriteTimeout = (int)TimeSpan.FromSeconds(timeoutSeconds).TotalMilliseconds
        };

        var writer = WriteAll(wrapper, bytes);
        var reader = ReadPauseRead(input, bytes / 2, TimeSpan.FromSeconds(timeoutSeconds + 0.5), bytes / 2);

        try
        {
            await writer;
        }
        catch (TimeoutException)
        {
            // Expected
            return;
        }

        Assert.Fail("TimeoutException not thrown");
    }

    private static Task<int> ReadAll(Stream stream, int bytes)
    {
        var buffer = new byte[1024 * 8];
        return RepeatRead(stream, buffer, bytes);
    }

    private static async Task WriteAll(Stream stream, int bytes)
    {
        var buffer = new byte[1024];
        while (bytes > 0)
        {
            var toWrite = Math.Min(buffer.Length, bytes);
            await stream.WriteAsync(buffer, 0, toWrite);
            bytes -= toWrite;
        }
    }

    private static async Task WritePauseWrite(Stream target, long bytesBefore, TimeSpan pause, long bytesAfter)
    {
        var buffer = new byte[bytesBefore];
        await target.WriteAsync(buffer);
        await Task.Delay(pause);
        buffer = new byte[bytesAfter];
        await target.WriteAsync(buffer);
    }

    private static async Task<int> RepeatRead(Stream stream, byte[] buffer, long readCount)
    {
        int total = 0;
        while (true)
        {
            var read = await stream.ReadAsync(buffer.AsMemory(0, (int)Math.Min(buffer.Length, readCount)));
            total += read;
            readCount -= read;

            if (read == 0 || readCount <= 0)
                break;
        }

        return total;
    }

    private static async Task<int> ReadPauseRead(Stream source, long bytesBefore, TimeSpan pause, long bytesAfter)
    {
        var buffer = new byte[1024 * 8];
        var count = await RepeatRead(source, buffer, bytesBefore);
        await Task.Delay(pause);
        count += await RepeatRead(source, buffer, bytesAfter);

        return count;
    }

    [Test]
    [TestCase(5000, 2000)]
    [TestCase(1000, 600)]
    [TestCase(2000, 10)]
    public async Task StartTimeoutShouldAllowFirstOperationWithinTimeout(int startTimeoutMilliseconds,
        int startDelayMilliseconds)
    {
        var pipe = new Pipe();
        var output = pipe.Writer.AsStream();
        var input = pipe.Reader.AsStream();

        var bytes = 1024 * 1024 * 2;

        var slowStream = new SlowStartingStream(input, TimeSpan.FromMilliseconds(startDelayMilliseconds));

        var wrapper = new TimeoutObservingStream(slowStream)
        {
            StartTimeout = (int)TimeSpan.FromMilliseconds(startTimeoutMilliseconds).TotalMilliseconds
        };

        var writer = WriteAll(output, bytes);
        var result = await ReadChunk(wrapper, bytes);
    }


    [Test]
    [TestCase(5000, 6000)]
    [TestCase(1000, 1600)]
    [TestCase(2000, 3010)]
    public async Task StartTimeoutShouldAllowFirstOperationWithinTimeoutShouldThrow(int startTimeoutMilliseconds,
        int startDelayMilliseconds)
    {
        var pipe = new Pipe();
        var output = pipe.Writer.AsStream();
        var input = pipe.Reader.AsStream();

        var bytes = 1024 * 1024 * 2;

        var slowStream = new SlowStartingStream(input, TimeSpan.FromMilliseconds(startDelayMilliseconds));

        var wrapper = new TimeoutObservingStream(slowStream)
        {
            StartTimeout = (int)TimeSpan.FromMilliseconds(startTimeoutMilliseconds).TotalMilliseconds
        };

        var writer = WriteAll(output, bytes);

        try
        {
            var result = await ReadChunk(wrapper, bytes);
        }
        catch (TimeoutException)
        {
            // Expected
            return;
        }

        Assert.Fail("TimeoutException not thrown");
    }


    [Test]
    [TestCase(1000*60)]
    public async Task StartTimeoutInfiniteShouldNeverTimeout(int startDelayMilliseconds)
    {
        var pipe = new Pipe();
        var output = pipe.Writer.AsStream();
        var input = pipe.Reader.AsStream();

        var bytes = 1024 * 1024 * 2;

        var slowStream = new SlowStartingStream(input, TimeSpan.FromMilliseconds(startDelayMilliseconds));

        var wrapper = new TimeoutObservingStream(slowStream)
        {
            StartTimeout = Timeout.Infinite
        };

        var writer = WriteAll(output, bytes);

        try
        {
            var result = await ReadChunk(wrapper, bytes);
        }
        catch (TimeoutException)
        {
            Assert.Fail("TimeoutException was thrown, should not have timed out");
        }
    }

    private static async Task<int> ReadChunk(Stream stream, int bytes)
    {
        var buffer = new byte[1024 * 8];
        int totalRead = 0;

        while (totalRead < bytes)
        {
            var toRead = Math.Min(buffer.Length, bytes - totalRead);
            var read = await stream.ReadAsync(buffer.AsMemory(0, toRead));
            if (read == 0) break;
            totalRead += read;
        }

        return totalRead;
    }


    /// <summary>
    /// A stream that wraps another stream and throttles read and write operations.
    /// </summary>
    private sealed class SlowStartingStream(Stream stream, TimeSpan startDelay) : WrappingStream(stream)
    {
        private bool _delayed = false;

        public override int Read(byte[] buffer, int offset, int count)
        {
            if (!_delayed)
            {
                Console.WriteLine(DateTime.Now.ToString("mm:ss ffff") +
                                  $"non async delaying {startDelay.Milliseconds} ms");
                Task.Delay(startDelay);
                _delayed = true;
            }

            return BaseStream.Read(buffer, offset, count);
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            if (!_delayed)
            {
                Console.WriteLine(DateTime.Now.ToString("mm:ss ffff") +
                                  $"non async delaying {startDelay.Milliseconds} ms");
                Task.Delay(startDelay);
                _delayed = true;
            }

            BaseStream.Write(buffer, offset, count);
        }

        public override async Task<int> ReadAsync(byte[] buffer, int offset, int count,
            CancellationToken cancellationToken)
        {
            if (!_delayed)
            {
                Console.WriteLine(DateTime.Now.ToString("mm:ss ffff") + $"delaying {startDelay.Milliseconds} ms");
                await Task.Delay(startDelay, cancellationToken);
                _delayed = true;
            }

            Console.WriteLine(DateTime.Now.ToString("mm:ss ffff") + " reading..");
            int read = await BaseStream.ReadAsync(buffer, offset, count, cancellationToken);
            Console.WriteLine(DateTime.Now.ToString("mm:ss ffff") + $" reading done with {read} bytes..");
            return read;
        }

        public override async Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            if (!_delayed)
            {
                Console.WriteLine(DateTime.Now.ToString("mm:ss ffff") + $"delaying {startDelay.Milliseconds} ms");
                await Task.Delay(startDelay, cancellationToken);
                _delayed = true;
            }

            await BaseStream.WriteAsync(buffer, offset, count, cancellationToken);
        }
    }
}