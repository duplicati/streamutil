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
/// A stream that can observe timeouts for read and write operations.
/// </summary>
public sealed class TimeoutObservingStream : WrappingAsyncStream
{
    /// <summary>
    /// The read timeout.
    /// </summary>
    private int _readTimeout = Timeout.Infinite;

    /// <summary>
    /// The write timeout.
    /// </summary>
    private int _writeTimeout = Timeout.Infinite;

    /// <summary>
    /// A timeout used as grace period for the other timeouts.
    ///
    /// Once the first transfer happens (read or write), this timeout is ignored.
    /// </summary>
    private int _startTimeout = Timeout.Infinite;

    /// <summary>
    /// The cancellation token source for the timeout.
    /// </summary>
    private readonly CancellationTokenSource _timeoutCts = new();

    /// <summary>
    /// The timer for the read timeout.
    /// </summary>
    private readonly Timer _readTimer;

    /// <summary>
    /// The timer for the write timeout.
    /// </summary>
    private readonly Timer _writeTimer;

    /// <summary>
    /// The time for the start timeout (grace period).
    /// </summary>
    private readonly Timer _startTimer;

    /// <summary>
    /// Initializes a new instance of the <see cref="TimeoutObservingStream"/> class.
    /// </summary>
    /// <param name="stream">The stream to wrap.</param>
    public TimeoutObservingStream(Stream stream)
        : base(stream)
    {
        _readTimer = new(_ => _timeoutCts.Cancel());
        _writeTimer = new(_ => _timeoutCts.Cancel());
        _startTimer = new(_ => _timeoutCts.Cancel());
    }

    /// <summary>
    /// The cancellation token for the timeout.
    /// </summary>
    public CancellationToken TimeoutToken => _timeoutCts.Token;

    /// <inheritdoc/>
    override public bool CanTimeout => true;

    /// <inheritdoc/>
    public override int ReadTimeout
    {
        get => _readTimeout;
        set
        {
            if (value <= 0 && value != Timeout.Infinite)
                throw new ArgumentOutOfRangeException(nameof(value));
            _readTimeout = value;
            _readTimer.Change(value, Timeout.Infinite);
        }
    }

    /// <inheritdoc/>
    public override int WriteTimeout
    {
        get => _writeTimeout;
        set
        {
            if (value <= 0 && value != Timeout.Infinite)
                throw new ArgumentOutOfRangeException(nameof(value));
            _writeTimeout = value;
            _writeTimer.Change(value, Timeout.Infinite);
        }
    }

    /// <summary>
    /// Configure value for the start timeout.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException"></exception>
    public int StartTimeout
    {
        get => _startTimeout;
        set
        {
            if (value <= 0 && value != Timeout.Infinite)
                throw new ArgumentOutOfRangeException(nameof(value));
            _startTimeout = value;
        }
    }

    /// <summary>
    /// Sets the timeout for both read and write operations to infinite as well as startTimeout
    /// </summary>
    public void CancelTimeout()
        => StartTimeout = WriteTimeout = ReadTimeout = Timeout.Infinite;

    private void CancelStartTimeout()
    {
        _hasCompletedOperation = true;
        _startTimer.Change(Timeout.Infinite, Timeout.Infinite);
    }

    private volatile bool _hasCompletedOperation;

    protected override async Task<int> ReadImplAsync(byte[] buffer, int offset, int count,
        CancellationToken cancellationToken)
    {
        try
        {
            // Start the grace period timer if this is the first operation and timeout is set
            if (!_hasCompletedOperation && _startTimeout != Timeout.Infinite)
                _startTimer.Change(_startTimeout, Timeout.Infinite);

            // If the read timer is enabled, restart it
            if (_readTimeout != Timeout.Infinite) _readTimer.Change(_readTimeout, Timeout.Infinite);

            // If there is no timeout and no cancellation token, we can just call the base stream
            if (_readTimeout == Timeout.Infinite &&
                _startTimeout == Timeout.Infinite &&
                _startTimeout == Timeout.Infinite &&
                !cancellationToken.CanBeCanceled)
            {
                var readResult = await BaseStream.ReadAsync(buffer, offset, count, cancellationToken)
                    .ConfigureAwait(false);
                CancelStartTimeout();
                return readResult;
            }

            // We need a cts here to handle cancellation when not handled by the callee
            using var cts = cancellationToken.CanBeCanceled && cancellationToken != TimeoutToken
                ? CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _timeoutCts.Token)
                : null;

            var tk = cts?.Token ?? _timeoutCts.Token;
            var task = BaseStream.ReadAsync(buffer, offset, count, tk);

            // If the task is already completed, we can await it without a timeout
            if (task.IsCompleted)
            {
                var completedResult = await task.ConfigureAwait(false);
                CancelStartTimeout();
                return completedResult;
            }

            // Run the task and observe the cancellation token
            var res = await Task.WhenAny(Task.Run(() => task, tk)).ConfigureAwait(false);

            // Check if we should throw a timeout exception
            if (!cancellationToken.IsCancellationRequested && _timeoutCts.IsCancellationRequested)
                throw new TimeoutException();

            // Any exceptions from the task are rethrown here
            var finalResult = await res.ConfigureAwait(false);
            CancelStartTimeout();
            return finalResult;
        }
        catch (OperationCanceledException exception)
        {
            throw new TimeoutException();
        }
    }

    protected override async Task WriteImplAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
        try
        {
            // Start the grace period timer if this is the first operation and timeout is set
            if (!_hasCompletedOperation && _startTimeout != Timeout.Infinite) _startTimer.Change(_startTimeout, Timeout.Infinite);

            // If the write timer is enabled, restart it
            if (_writeTimeout != Timeout.Infinite) _writeTimer.Change(_writeTimeout, Timeout.Infinite);

            // If there is no timeout and no cancellation token, we can just call the base stream
            if (_writeTimeout == Timeout.Infinite &&
                _startTimeout == Timeout.Infinite &&
                _startTimeout == Timeout.Infinite &&
                !cancellationToken.CanBeCanceled)
            {
                await BaseStream.WriteAsync(buffer, offset, count, cancellationToken)
                    .ConfigureAwait(false);
                CancelStartTimeout();
                return;
            }

            // We need a cts here to handle cancellation when not handled by the callee
            using var cts = cancellationToken.CanBeCanceled && cancellationToken != TimeoutToken
                ? CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _timeoutCts.Token)
                : null;

            var tk = cts?.Token ?? _timeoutCts.Token;
            var task = BaseStream.WriteAsync(buffer, offset, count, tk);

            // If the task is already completed, we can await it without a timeout
            if (task.IsCompleted)
            {
                await task.ConfigureAwait(false);
                CancelStartTimeout();
                return;
            }

            // Run the task and observe the cancellation token
            var res = await Task.WhenAny(Task.Run(() => task, tk)).ConfigureAwait(false);

            // Check if we should throw a timeout exception
            if (!cancellationToken.IsCancellationRequested && _timeoutCts.IsCancellationRequested)
                throw new TimeoutException();

            // Any exceptions from the task are rethrown here
            await res.ConfigureAwait(false);
            CancelStartTimeout();
        }
        catch (OperationCanceledException exception)
        {
            throw new TimeoutException();
        }
    }

    /// <inheritdoc/>
    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            _readTimer.Dispose();
            _writeTimer.Dispose();
            _timeoutCts.Dispose();
            _startTimer.Dispose();
        }

        base.Dispose(disposing);
    }
}