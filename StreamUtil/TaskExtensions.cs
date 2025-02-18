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
/// Task extension methods.
/// </summary>
internal static class TaskExtensions
{
    /// <summary>
    /// Adds a timeout to a task.
    /// Note that this method does not cancel the task, it only throws a <see cref="TimeoutException"/> if the task does not complete within the specified time.
    /// </summary>
    /// <param name="task">The task to wait for.</param>
    /// <param name="millisecondsTimeout">The timeout in milliseconds.</param>
    /// <param name="cancellationToken">The token to monitor for cancellation requests.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    /// <exception cref="TimeoutException">The task did not complete within the specified time.</exception>
    /// <exception cref="OperationCanceledException">The operation was canceled.</exception>
    public static async Task TimeoutAfter(Task task, int millisecondsTimeout, CancellationToken cancellationToken)
    {
        if (task.IsCompleted || (millisecondsTimeout == Timeout.Infinite && !cancellationToken.CanBeCanceled))
        {
            await task;
            return;
        }

        if (millisecondsTimeout == 0)
            throw new TimeoutException();

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var ts = Task.Delay(millisecondsTimeout, cts.Token);
        if (await Task.WhenAny(ts, task) == ts)
        {
            if (cancellationToken.IsCancellationRequested)
                throw new OperationCanceledException(cancellationToken);

            throw new TimeoutException();
        }

        // Stop the timer
        cts.Cancel();

        // Rethrow any exceptions from the task
        await task;
    }

    /// <summary>
    /// Adds a timeout to a task.
    /// Note that this method does not cancel the task, it only throws a <see cref="TimeoutException"/> if the task does not complete within the specified time.
    /// </summary>
    /// <typeparam name="T">The type of the result produced by the task.</typeparam>
    /// <param name="task">The task to wait for.</param>
    /// <param name="millisecondsTimeout">The timeout in milliseconds.</param>
    /// <param name="cancellationToken">The token to monitor for cancellation requests.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    /// <exception cref="TimeoutException">The task did not complete within the specified time.</exception>
    /// <exception cref="OperationCanceledException">The operation was canceled.</exception>
    public static async Task<T> TimeoutAfter<T>(Task<T> task, int millisecondsTimeout, CancellationToken cancellationToken)
    {
        if (task.IsCompleted || (millisecondsTimeout == Timeout.Infinite && !cancellationToken.CanBeCanceled))
            return await task;

        if (millisecondsTimeout == 0)
            throw new TimeoutException();

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var ts = Task.Delay(millisecondsTimeout, cts.Token);
        if (await Task.WhenAny(ts, task) == ts)
        {
            if (cancellationToken.IsCancellationRequested)
                throw new OperationCanceledException(cancellationToken);

            throw new TimeoutException();
        }

        // Stop the timer
        cts.Cancel();

        // Rethrow any exceptions from the task
        return await task;
    }
}

