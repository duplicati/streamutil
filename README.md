# Stream utility

Stream wrappers that add functionality to the .NET base streams.

## Helpers: `WrappingStream` and `WrappingAsyncStream`

It contains the [`WrappingStream`](./StreamUtil/WrappingStream.cs) abstract class that simplifies wrapping another `Stream` by delegating all calls to that underlying stream. The [`WrappingAsyncStream`](./StreamUtil/WrappingAsyncStream.cs) is similar by changes the required implementation to be the async read/write methods and lets the synchronous methods use the `async` implementation by default.

## Timeout support

On top of these helpers, the [`TimeoutObservingStream`](./StreamUtil/TimeoutObservingStream.cs) adds the missing timeout functionality to streams, such that streams operations can actually be cancelled via the `CancellationToken` as well as the timeout property, which is not fully supported for ordinary streams. Notably, `HttpClient` does not support timeout on the streams and the `System.IO.Stream.ReadAsync` / `System.IO.Stream.WriteAsync` only checks the cancellation token on entry and ignores it during and after the operation.

This is problematic in situations where the expected transfer time is unknown, such as uploading or downloading large files. Because the network and remote site has unknown bandwidth requirements, it is not possible to determine in advance how long a timeout to use for the entire operation. One approach to this problem is to ensure that some progress is made, meaning that each operation has to complete within a set time, and if it does, it means that progress is made. Only in the event where progress stops or slows beyound a certain threshold is the connection stopped.

The `ReadTimeout` and `WriteTimeout` can be set during use, but is only read when entering a read or write operation. In other words: setting timeout while a read/write operation is active will take effect on the next read/write call.

### What the timeout measures

A transfer has two sides: the wrapped stream, which answers each read or write, and the caller, which decides when the next read or write happens. Usually only one of them is the remote side whose silence should fail the transfer, and time spent waiting for the other side is normal flow control. The `Mode` property selects which time counts:

| `TimeoutMode`          | Timer runs                                                            | Use when                                                                                                                       |
| ---------------------- | --------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------ |
| `CallAndGap` (default) | From the start of a call until the start of the next call             | The original behavior: no call may start more than the timeout after the previous one, however the time was spent              |
| `Call`                 | Only while a read or write is in progress                             | The wrapped stream is the remote side, such as a network response stream, and the caller may pause between calls               |
| `Gap`                  | Only between calls, and from setting the timeout until the first call | The stream is handed to a library that does the network transfer, and the wrapped stream is local or may block on flow control |

For example, a process that reads from a remote source and writes to a slow destination will hold its reader back while the destination catches up. With `CallAndGap` that pause counts as a stalled source; with `Call` on the response stream, or `Gap` on a stream handed to the transfer library, it does not.

Each timer covers one direction, so set only the timeout for the direction in use. A timeout is permanent: once it has fired, every later read or write fails with a `TimeoutException`.

## Throttle (aka rate-limit, aka bandwidth control)

The [`ThrottleEnabledStream`](./StreamUtil/ThrottleEnabledStream.cs) allows the caller to throttle (aka rate-limit) the read and/or write speed by inserting small pauses, either with `Thread.Sleep()` or `await Task.Delay()` for `sync`/`async` respectively. A throttle value of zero will disable the throttle.

If needed, the [`ThrottleManager`](./StreamUtil/ThrottleManager.cs) can be shared among multiple `ThrottleEnabledStream` instances allowing a subset of streams to share a set bandwidth limit.

## Composable stream

The [`LambdaInterceptStream`](./StreamUtil/LambdaInterceptStream.cs) is not implemented with performance in mind, but is useful for implementing various test or logging setups by wrapping a stream an allowing weaved methods before and after calls.

# Example use

```csharp
var fileStream = File.Open(...);
var timeoutStream = new TimeoutObservingStream(fileStream) {
    ReadTimeout = (int)TimeSpan.FromSeconds(3).TotalMilliSeconds
};

var ms = new MemoryStream();
var cts = new CancellationTokenSource();

// Any
await timeoutStream.CopyAsync(ms, cts.Token);
```
