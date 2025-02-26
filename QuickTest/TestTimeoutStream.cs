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

using System.Net;

namespace Duplicati.StreamUtil.QuickTest;

public static class TestTimeoutStream
{
    public static async Task Run()
    {
        var data = new MemoryStream();
        data.SetLength(1024 * 1024 * 100);
        data.Position = 0;

        const int Port = 12345;
        var sinkCts = new CancellationTokenSource();
        var sinkTask = SocketHelper.SinkConnectionHttpResponse(Port, sinkCts.Token, data.Length, async b => Console.WriteLine($"Received {b} bytes"));


        var readCount = 0;
        var iteration = 0;

        var pausingStream = new LambdaInterceptStream(data)
        {
            PostReadTask = (buffer, offset, count, read) =>
            {
                readCount += read;
                if (readCount < iteration * 1024 * 1024)
                    return;

                iteration++;
                Console.WriteLine($"Pausing {iteration} secs after reading {readCount} bytes");
                Thread.Sleep(1000 * iteration);
            }
        };

        var wrappedStream = new TimeoutObservingStream(data)
        {
            ReadTimeout = 3000
        };

        var completingStream = new CompletingStream(wrappedStream, () => wrappedStream.CancelTimeout());

        var uri = new Uri($"http://localhost:{Port}/");
        var userInfo = new NetworkCredential("user", "pass");

        var httpClient = new HttpClient(new HttpClientHandler
        {
            // PreAuthenticate = true,
            // UseDefaultCredentials = true,
            Credentials = new CredentialCache
            {
                { uri, "Digest", userInfo }
            }
            //Credentials = new NetworkCredential("user", "pass")
        })
        {
            Timeout = Timeout.InfiniteTimeSpan,
            BaseAddress = uri
        };

        var request = new HttpRequestMessage(HttpMethod.Post, $"")
        {
            Content = new StreamContent(completingStream)
        };

        try
        {
            var response = await httpClient.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, wrappedStream.TimeoutToken);
            var stream = new TimeoutObservingStream(response.Content.ReadAsStream()) { ReadTimeout = 3000 };
            Console.WriteLine($"Stream type: {stream.GetType().Name}");
            Console.WriteLine($"Stream can timeout: {stream.CanTimeout}");
        }
        catch (TimeoutException)
        {
            Console.WriteLine("Timeout exception");
        }
    }

    private class DelayHelper
    {
        private long bytesReceived = 0;
        private int nextThreshold = 1024 * 1024;
        private int round = 0;

        public async Task Delay1sAfterEachMb(Memory<byte> data)
        {
            bytesReceived += data.Length;
            if (bytesReceived < nextThreshold)
                return;

            round++;
            Console.WriteLine($"Received {data.Length} bytes, total received: {bytesReceived}, round: {round}");
            nextThreshold += 1024 * 1024;
            await Task.Delay(1000 * round);
        }
    }
}
