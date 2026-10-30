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

namespace Duplicati.StreamUtil;

/// <summary>
/// Selects which time a <see cref="TimeoutObservingStream"/> counts against its timeouts.
/// </summary>
/// <remarks>
/// A transfer has two sides: the wrapped stream, which answers each read or write,
/// and the caller, which decides when the next read or write happens.
/// Only one of them is usually the remote side whose silence should fail the transfer.
/// Time spent waiting on the other side is normal flow control and should not count.
/// </remarks>
public enum TimeoutMode
{
    /// <summary>
    /// The timer is restarted when a call starts and keeps running until the next call starts,
    /// so it covers the time inside each call and the gap between calls.
    /// It fails the transfer when no call has started within the timeout, however the time was spent.
    /// This is the original behavior.
    /// </summary>
    CallAndGap,

    /// <summary>
    /// The timer runs only while a read or write is in progress.
    /// It fails the transfer when the wrapped stream does not answer a call within the timeout,
    /// and ignores how long the caller waits between calls.
    /// Use this when the wrapped stream is the remote side, such as a network response stream.
    /// </summary>
    Call,

    /// <summary>
    /// The timer runs only between calls, from the return of one call to the start of the next,
    /// and from the moment the timeout is set until the first call.
    /// It fails the transfer when the caller does not come back within the timeout,
    /// and ignores how long the wrapped stream takes to answer a call.
    /// Use this when the stream is handed to a library that does the network transfer,
    /// and the wrapped stream is local or may block on flow control.
    /// </summary>
    Gap
}
