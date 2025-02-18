namespace Duplicati.StreamUtil;

/// <summary>
/// Stream that reports the speed of data transfer.
/// </summary>
public class SpeedMeasuringStream : WrappingStream
{
    /// <summary>
    /// The interval between each sample.
    /// </summary>
    private readonly long _sampleInterval = TimeSpan.FromSeconds(1).Ticks;
    /// <summary>
    /// The samples of data transfer rates.
    /// </summary>
    private readonly long[] _samples = new long[10];
    /// <summary>
    /// The offset of the current sample.
    /// </summary>
    private long _sampleBucket = 0;
    /// <summary>
    /// The number of samples collected.
    /// </summary>
    private int _sampleCount = 0;
    /// <summary>
    /// The total number of bytes transferred.
    /// </summary>
    private long _totalBytes = 0;
    /// <summary>
    /// The start time of the first sample.
    /// </summary>
    private DateTime _startMeasureTime = DateTime.Now;

    /// <summary>
    /// Creates a new SpeedReportingStream.
    /// </summary>
    /// <param name="baseStream">The stream to wrap.</param>
    public SpeedMeasuringStream(Stream baseStream)
        : base(baseStream)
    {
    }

    /// <summary>
    /// The total number of bytes processed.
    /// </summary>
    public long BytesProcessed => _totalBytes;

    /// <summary>
    /// The total number of bytes processed per second.
    /// </summary>
    public long TotalBytesPerSecond
    {
        get
        {
            var seconds = (DateTime.Now - _startMeasureTime).TotalSeconds;
            return seconds < 0.5 ? 0 : (long)(_totalBytes / seconds);
        }
    }

    /// <summary>
    /// The number of bytes processed per second in the recent window.
    /// </summary>
    public long RecentBytesPerSecond
    {
        get
        {
            // Skip the current sample, it's not complete
            var samples = _sampleCount - 1;
            var firstbucket = GetSampleBucket() - samples - 1;
            if (samples < 1 || firstbucket < 0)
                return 0;

            // Very first sample is wrong
            if (_sampleCount < _samples.Length)
                firstbucket++;

            var total = 0L;
            for (var i = 0; i < samples; i++)
                total += _samples[(firstbucket + i) % _samples.Length];

            return total / (samples * (_sampleInterval / TimeSpan.TicksPerSecond));
        }
    }

    /// <summary>
    /// Calculates which sample bucket the current time falls into.
    /// </summary>
    /// <returns>The offset of the current sample.</returns>
    private long GetSampleBucket()
        => (DateTime.Now - _startMeasureTime).Ticks / _sampleInterval;

    /// <summary>
    /// Adds a measurement to the samples.
    /// </summary>
    /// <param name="bytes">The number of bytes transferred.</param>
    private void AddMeasurement(long bytes)
    {
        var bucket = GetSampleBucket();
        var index = (int)(bucket % _samples.Length);
        _totalBytes += bytes;

        // Same sample bucket
        if (bucket == _sampleBucket)
        {
            _samples[index] += bytes;
        }
        // Next sample bucket
        else if (bucket == _sampleBucket + 1)
        {
            _samples[index] = bytes;
            if (_sampleCount < _samples.Length)
                _sampleCount++;
        }
        // Some empty buckets
        else if (bucket <= _sampleBucket + (_samples.Length / 2))
        {
            // Fill in missing samples
            for (var i = _sampleBucket; i < bucket; i++)
            {
                _samples[i % _samples.Length] = 0;
                if (_sampleCount < _samples.Length)
                    _sampleCount++;
            }

            _samples[index] = bytes;
        }
        // Give up, reset everything
        else
        {
            // Reset
            _startMeasureTime = DateTime.Now;
            bucket = 0;
            Array.Fill(_samples, 0);
            _sampleCount = 1;
            _samples[index] = bytes;
        }

        _sampleBucket = bucket;
    }

    /// <summary>
    /// Resets the counters.
    /// </summary>
    public void ResetCounters()
    {
        _totalBytes = 0;
        _sampleBucket = 0;
        _sampleCount = 0;
        Array.Fill(_samples, 0);
        _startMeasureTime = DateTime.Now;
    }

    /// <inheritdoc/>
    public override int Read(byte[] buffer, int offset, int count)
    {
        var read = BaseStream.Read(buffer, offset, count);
        AddMeasurement(read);
        return read;
    }

    /// <inheritdoc/>
    public override void Write(byte[] buffer, int offset, int count)
    {
        BaseStream.Write(buffer, offset, count);
        AddMeasurement(count);
    }
}
