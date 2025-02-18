namespace Duplicati.StreamUtil;

/// <summary>
/// Implements a stream that invokes a callback when it completes.
/// </summary>
public class CompletingStream : WrappingStream
{
    /// <summary>
    /// The expected length of the stream.
    /// </summary>
    private readonly long _length;
    /// <summary>
    /// The number of bytes read or written so far.
    /// </summary>
    private long _operationCount;
    /// <summary>
    /// Whether the stream has completed.
    /// </summary>
    private bool _completed;
    /// <summary>
    /// The callback to invoke when the stream completes.
    /// </summary>
    private readonly Action _onComplete;

    /// <summary>
    /// Initializes a new instance of the <see cref="CompletingStream"/> class.
    /// </summary>
    /// <param name="baseStream">The stream to wrap.</param>
    /// <param name="length">The expected length of the stream.</param>
    /// <param name="onComplete">The callback to invoke when the stream completes.</param>
    public CompletingStream(Stream baseStream, long length, Action onComplete)
        : base(baseStream)
    {
        _length = length;
        _onComplete = onComplete;
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="CompletingStream"/> class.
    /// </summary>
    /// <param name="baseStream">The stream to wrap.</param>
    /// <param name="onComplete">The callback to invoke when the stream completes.</param>
    public CompletingStream(Stream baseStream, Action onComplete)
        : this(baseStream, baseStream.Length, onComplete)
    {
    }

    /// <inheritdoc/>
    public override int Read(byte[] buffer, int offset, int count)
    {
        var bytesRead = BaseStream.Read(buffer, offset, count);
        _operationCount += bytesRead;

        if (_operationCount >= _length && !_completed)
        {
            _completed = true;
            _onComplete();
        }

        return bytesRead;
    }

    /// <inheritdoc/>
    public override void Write(byte[] buffer, int offset, int count)
    {
        BaseStream.Write(buffer, offset, count);
        _operationCount += count;

        if (_operationCount >= _length && !_completed)
        {
            _completed = true;
            _onComplete();
        }
    }
}
