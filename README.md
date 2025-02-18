# Stream utility

Stream wrappers that add functionality to the .NET base streams.

## Helpers: `WrappingStream` and `WrappingAsyncStream`

It contains the [`WrappingStream`](./StreamUtil/WrappingStream.cs) abstract class that simplifies wrapping another `Stream` by delegating all calls to that underlying stream. The [`WrappingAsyncStream`](./StreamUtil/WrappingAsyncStream.cs) is similar by changes the required implementation to be the async read/write methods and lets the synchronous methods use the `async` implementation by default.

## Timeout support

On top of these helpers, the [`TimeoutObservingStream`](./StreamUtil/TimeoutObservingStream.cs) adds the missing timeout functionality to streams, such that streams operations can actually be cancelled via the `CancellationToken` as well as the timeout property, which is not fully supported for ordinary streams. Notably, `HttpClient` does not support timeout on the streams and the `System.IO.Stream.ReadAsync` / `System.IO.Stream.WriteAsync` only checks the cancellation token on entry and ignores it during and after the operation.

This is problematic in situations where the expected transfer time is unknown, such as uploading or downloading large files. Because the network and remote site has unknown bandwidth requirements, it is not possible to determine in advance how long a timeout to use for the entire operation. One approach to this problem is to ensure that some progress is made, meaning that each operation has to complete within a set time, and if it does, it means that progress is made. Only in the event where progress stops or slows beyound a certain threshold is the connection stopped.

The `ReadTimeout` and `WriteTimeout` can be set during use, but is only read when entering a read or write operation. In other words: setting timeout while a read/write operation is active will take effect on the next read/write call.

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
