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
    /// The time that counts against the timeouts.
    /// </summary>
    private TimeoutMode _mode = TimeoutMode.CallAndGap;

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
        : this(stream, true) { }

    /// <summary>
    /// Initializes a new instance of the <see cref="TimeoutObservingStream"/> class.
    /// </summary>
    /// <param name="stream">The stream to wrap.</param>
    /// <param name="disposeBaseStream">Whether to dispose the base stream when this stream is disposed.</param>
    public TimeoutObservingStream(Stream stream, bool disposeBaseStream)
        : base(stream, disposeBaseStream)
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

    /// <summary>
    /// The time that counts against <see cref="ReadTimeout"/> and <see cref="WriteTimeout"/>.
    /// The default is <see cref="TimeoutMode.CallAndGap"/>.
    /// </summary>
    /// <remarks>
    /// Set the mode before the timeouts, or while no read or write is in progress.
    /// </remarks>
    public TimeoutMode Mode
    {
        get => _mode;
        set
        {
            if (!Enum.IsDefined(value))
                throw new ArgumentOutOfRangeException(nameof(value));
            _mode = value;
            ArmBetweenCalls(_readTimer, _readTimeout);
            ArmBetweenCalls(_writeTimer, _writeTimeout);
        }
    }

    /// <inheritdoc/>
    public override int ReadTimeout
    {
        get => _readTimeout;
        set
        {
            if (value <= 0 && value != Timeout.Infinite)
                throw new ArgumentOutOfRangeException(nameof(value));
            _readTimeout = value;
            ArmBetweenCalls(_readTimer, value);
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
            ArmBetweenCalls(_writeTimer, value);
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

    /// <summary>
    /// Sets a timer to the state it has while no call is in progress:
    /// running in the modes that count the gaps between calls, stopped otherwise.
    /// </summary>
    /// <param name="timer">The timer to set.</param>
    /// <param name="timeout">The timeout for the timer.</param>
    private void ArmBetweenCalls(Timer timer, int timeout)
        => timer.Change(_mode == TimeoutMode.Call ? Timeout.Infinite : timeout, Timeout.Infinite);

    /// <summary>
    /// Sets a timer to the state it has while a call is in progress:
    /// restarted in the modes that count the time inside calls, stopped otherwise.
    /// </summary>
    /// <param name="timer">The timer to set.</param>
    /// <param name="timeout">The timeout for the timer.</param>
    private void ArmDuringCall(Timer timer, int timeout)
        => timer.Change(_mode == TimeoutMode.Gap ? Timeout.Infinite : timeout, Timeout.Infinite);

    /// <summary>
    /// Runs a read or write on the base stream while observing its timeout, the start timeout and the cancellation token.
    /// </summary>
    /// <typeparam name="T">The result type of the operation.</typeparam>
    /// <param name="timer">The timer for the operation.</param>
    /// <param name="timeout">The timeout for the operation.</param>
    /// <param name="operation">The operation to run on the base stream.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The result of the operation.</returns>
    private async Task<T> ObserveAsync<T>(Timer timer, int timeout, Func<CancellationToken, Task<T>> operation, CancellationToken cancellationToken)
    {
        // A timeout that has fired, during an earlier call or in the gap since it, is permanent
        if (_timeoutCts.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
            throw new TimeoutException();

        try
        {
            // Start the grace period timer if this is the first operation and timeout is set
            if (!_hasCompletedOperation && _startTimeout != Timeout.Infinite)
                _startTimer.Change(_startTimeout, Timeout.Infinite);

            ArmDuringCall(timer, timeout);

            // If there is no timeout and no cancellation token, we can just call the base stream
            if (timeout == Timeout.Infinite &&
                _startTimeout == Timeout.Infinite &&
                !cancellationToken.CanBeCanceled)
            {
                var directResult = await operation(cancellationToken).ConfigureAwait(false);
                CancelStartTimeout();
                return directResult;
            }

            // We need a cts here to handle cancellation when not handled by the callee
            using var cts = cancellationToken.CanBeCanceled && cancellationToken != TimeoutToken
                ? CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _timeoutCts.Token)
                : null;

            var tk = cts?.Token ?? _timeoutCts.Token;
            var task = operation(tk);

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
        catch (OperationCanceledException)
        {
            throw new TimeoutException();
        }
        finally
        {
            ArmBetweenCalls(timer, timeout);
        }
    }

    /// <inheritdoc/>
    protected override Task<int> ReadImplAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        => ObserveAsync(_readTimer, _readTimeout, ct => BaseStream.ReadAsync(buffer, offset, count, ct), cancellationToken);

    /// <inheritdoc/>
    protected override Task WriteImplAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        => ObserveAsync(_writeTimer, _writeTimeout, async ct =>
        {
            await BaseStream.WriteAsync(buffer, offset, count, ct).ConfigureAwait(false);
            return 0;
        }, cancellationToken);

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
