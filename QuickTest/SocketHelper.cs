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

using System.Net;
using System.Net.Sockets;
using System.Text;

namespace Duplicati.StreamUtil.QuickTest;

public class SocketHelper : IDisposable
{
    public class SocketHelperConnection : IDisposable
    {
        private readonly Socket _socket;

        public SocketHelperConnection(Socket socket)
        {
            _socket = socket;
        }

        public async Task<int> ReceiveAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            return await _socket.ReceiveAsync(new ArraySegment<byte>(buffer, offset, count), SocketFlags.None, cancellationToken);
        }

        public async Task SendAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {

            await _socket.SendAsync(new ArraySegment<byte>(buffer, offset, count), SocketFlags.None, cancellationToken);
        }

        public async Task SinkAsync(CancellationToken cancellationToken)
        {
            var buffer = new byte[1024];
            while (!cancellationToken.IsCancellationRequested)
            {
                var data = await ReceiveAsync(buffer, 0, buffer.Length, cancellationToken);
                if (data == 0)
                    break;

                Console.WriteLine($"Received {data} bytes");
                Console.WriteLine(Encoding.UTF8.GetString(buffer, 0, data));
                Console.WriteLine();
            }
        }

        public void Dispose()
        {
            _socket.Close();
            _socket.Dispose();
        }
    }

    private readonly Socket _socket;
    private readonly CancellationTokenSource _stopTokenSource = new();

    public SocketHelper(int port)
    {
        _socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        _socket.Bind(new IPEndPoint(IPAddress.Any, port));
        _socket.Listen(10);
    }

    public async Task<SocketHelperConnection> AcceptAsync()
        => new SocketHelperConnection(await _socket.AcceptAsync());

    public void Dispose()
    {
        _stopTokenSource.Cancel();
        _stopTokenSource.Dispose();
        _socket.Close();
        _socket.Dispose();
    }

    public static async Task SinkConnectionHttpResponse(int port, CancellationToken cancellationToken, long bytesToRead, Func<Memory<byte>, Task>? onReceive = null)
    {
        await SinkConnection(port, cancellationToken, bytesToRead, onReceive, () => Encoding.UTF8.GetBytes("HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n"));
    }

    public static async Task SinkConnection(int port, CancellationToken cancellationToken, long bytesToRead, Func<Memory<byte>, Task>? onReceive = null, Func<Memory<byte>>? createResponse = null)
    {
        using var socketHelper = new SocketHelper(port);
        var connection = await socketHelper.AcceptAsync();
        // await connection.SinkAsync(cancellationToken);
        var buffer = new byte[1024 * 10];
        while (!cancellationToken.IsCancellationRequested)
        {
            var data = await connection.ReceiveAsync(buffer, 0, (int)Math.Min(buffer.Length, bytesToRead), cancellationToken);
            if (data == 0)
                break;
            bytesToRead -= data;

            if (onReceive != null)
                await onReceive(buffer.AsMemory(0, data));

            if (bytesToRead <= 0)
                break;
        }

        if (createResponse != null)
        {
            var r = createResponse();
            await connection.SendAsync(r.ToArray(), 0, r.Length, cancellationToken);
        }
    }
}
