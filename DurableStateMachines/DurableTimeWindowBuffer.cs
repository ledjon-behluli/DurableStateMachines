using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using System.Collections;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace Ledjon.DurableStateMachines;

/// <summary>
/// Defines a durable buffer that stores items added within a specific time window.
/// When new items are added, any items older than the specified time window are automatically discarded.
/// </summary>
/// <typeparam name="T">Specifies the type of elements in the buffer.</typeparam>
public interface IDurableTimeWindowBuffer<T> : IEnumerable<T>, IReadOnlyCollection<T>
{
    /// <summary>
    /// Gets the duration that items are retained in the buffer.
    /// </summary>
    TimeSpan Window { get; }

    /// <summary>
    /// Gets a value indicating whether the buffer is empty.
    /// </summary>
    bool IsEmpty { get; }

    /// <summary>
    /// Sets the time window for the buffer.
    /// If the new window is smaller than the current one, older items may be discarded immediately.
    /// </summary>
    /// <param name="window">The desired new time window.</param>
    /// <returns><c>true</c> if the time window is different from the provided <paramref name="window"/>, which in turn becomes the new window; otherwise, <c>false</c>.</returns>
    /// <exception cref="ArgumentOutOfRangeException">Thrown if time window is zero or negative.</exception>
    bool SetWindow(TimeSpan window);

    /// <summary>
    /// Adds an item to the buffer with the current timestamp. Any items older than the time window are removed.
    /// </summary>
    /// <param name="item">The item to add to the buffer.</param>
    void Enqueue(T item);

    /// <summary>
    /// Tries to remove and return the oldest object in the buffer.
    /// </summary>
    /// <param name="result">
    /// When this method returns, if the operation was successful, <paramref name="result"/> contains the object removed.
    /// If the buffer was empty, <paramref name="result"/> is the default value of <typeparamref name="T"/>.
    /// </param>
    /// <returns><c>true</c> if an element was removed and returned from the buffer successfully; otherwise, <c>false</c>.</returns>
    bool TryDequeue([MaybeNullWhen(false)] out T result);

    /// <summary>
    /// Determines whether the buffer contains a specific value.
    /// <para>
    /// The comparison is performed using <see cref="EqualityComparer{T}.Default"/>.
    /// </para>
    /// </summary>
    /// <param name="item">The object to locate in the buffer. The value can be <c>null</c> for reference types.</param>
    /// <returns><c>true</c> if <paramref name="item"/> is found in the buffer; otherwise, <c>false</c>.</returns>
    bool Contains(T item);

    /// <summary>
    /// Copies the elements of the buffer to an array, starting at a particular array index.
    /// The elements are copied in their logical order (from oldest to newest).
    /// </summary>
    /// <param name="array">The destination array to copy items into.</param>
    /// <param name="arrayIndex">The zero-based index in the array at which copying begins.</param>
    /// <returns>The number of items copied to the destination array.</returns>
    /// <exception cref="ArgumentNullException">If the array is <c>null</c></exception>
    int CopyTo(T[] array, int arrayIndex);

    /// <summary>
    /// Copies the elements of the buffer to a span.
    /// The elements are copied in their logical order (from oldest to newest).
    /// </summary>
    /// <param name="destination">The destination span to copy items into.</param>
    /// <returns>The number of items copied to the destination span.</returns>
    int CopyTo(Span<T> destination);

    /// <summary>
    /// Copies all elements to the destination array and then clears the buffer.
    /// The elements are copied in their logical order (from oldest to newest).
    /// </summary>
    /// <param name="array">The destination array to drain items into.</param>
    /// <param name="arrayIndex">The zero-based index in the array at which draining begins.</param>
    /// <returns>The number of items drained to the destination array.</returns>
    /// <exception cref="ArgumentNullException">If the array is <c>null</c></exception>
    int DrainTo(T[] array, int arrayIndex);

    /// <summary>
    /// Copies all elements to the destination span and then clears the buffer.
    /// The elements are copied in their logical order (from oldest to newest).
    /// </summary>
    /// <param name="destination">The destination span to drain items into.</param>
    /// <returns>The number of items drained to the destination span.</returns>
    int DrainTo(Span<T> destination);

    /// <summary>
    /// Removes all items from the buffer.
    /// </summary>
    void Clear();
}

/// <summary>
/// Receives decoded durable time window buffer commands from a codec implementation.
/// </summary>
public interface IDurableTimeWindowBufferCommandHandler<T>
{
    /// <summary>Applies a set window command.</summary>
    void ApplySetWindow(long windowSeconds);

    /// <summary>Applies an enqueue command.</summary>
    void ApplyEnqueue(T item, long timestamp);

    /// <summary>Applies a try dequeue command.</summary>
    void ApplyTryDequeue();

    /// <summary>Applies a clear command.</summary>
    void ApplyClear();

    /// <summary>Resets the receiver before applying replacement entries.</summary>
    void Reset(long windowSeconds);
}

/// <summary>
/// Serializes one durable time window buffer command and applies one decoded command.
/// </summary>
public interface IDurableTimeWindowBufferCommandCodec<T>
{
    /// <summary>Writes a set window command.</summary>
    void WriteSetWindow(long windowSeconds, JournalStreamWriter writer);

    /// <summary>Writes an enqueue command.</summary>
    void WriteEnqueue(T item, long timestamp, JournalStreamWriter writer);

    /// <summary>Writes a dequeue command.</summary>
    void WriteDequeue(JournalStreamWriter writer);

    /// <summary>Writes a clear command.</summary>
    void WriteClear(JournalStreamWriter writer);

    /// <summary>Writes a snapshot command.</summary>
    void WriteSnapshot(long windowSeconds, int count, IEnumerable<(T Item, long Timestamp)> items, JournalStreamWriter writer);

    /// <summary>Reads one encoded command and applies it to <paramref name="handler"/>.</summary>
    void Apply(JournalBufferReader input, IDurableTimeWindowBufferCommandHandler<T> handler);
}

internal sealed class DurableTimeWindowBufferCommandBinaryCodec<T>(
    IFieldCodec<T> codec, SerializerSessionPool sessionPool) : IDurableTimeWindowBufferCommandCodec<T>
{
    private const byte VersionByte = 0;

    private enum CommandType : uint
    {
        Clear = 0,
        Snapshot = 1,
        SetWindow = 2,
        Enqueue = 3,
        Dequeue = 4
    }

    public void WriteSetWindow(long windowSeconds, JournalStreamWriter writer)
    {
        using var entry = writer.BeginEntry();
        using var session = sessionPool.GetSession();

        var payloadWriter = Writer.Create(entry.Writer, session);

        payloadWriter.WriteByte(VersionByte);
        payloadWriter.WriteVarUInt32((uint)CommandType.SetWindow);

        payloadWriter.WriteVarUInt64((ulong)windowSeconds);

        payloadWriter.Commit();
        entry.Commit();
    }

    public void WriteEnqueue(T item, long timestamp, JournalStreamWriter writer)
    {
        using var entry = writer.BeginEntry();
        using var session = sessionPool.GetSession();

        var payloadWriter = Writer.Create(entry.Writer, session);

        payloadWriter.WriteByte(VersionByte);
        payloadWriter.WriteVarUInt32((uint)CommandType.Enqueue);

        codec.WriteField(ref payloadWriter, 0, typeof(T), item);
        payloadWriter.WriteVarUInt64((ulong)timestamp);

        payloadWriter.Commit();
        entry.Commit();
    }

    public void WriteDequeue(JournalStreamWriter writer)
    {
        using var entry = writer.BeginEntry();
        using var session = sessionPool.GetSession();

        var payloadWriter = Writer.Create(entry.Writer, session);

        payloadWriter.WriteByte(VersionByte);
        payloadWriter.WriteVarUInt32((uint)CommandType.Dequeue);

        payloadWriter.Commit();
        entry.Commit();
    }

    public void WriteClear(JournalStreamWriter writer)
    {
        using var entry = writer.BeginEntry();
        using var session = sessionPool.GetSession();

        var payloadWriter = Writer.Create(entry.Writer, session);

        payloadWriter.WriteByte(VersionByte);
        payloadWriter.WriteVarUInt32((uint)CommandType.Clear);

        payloadWriter.Commit();
        entry.Commit();
    }

    public void WriteSnapshot(long windowSeconds, int count, IEnumerable<(T Item, long Timestamp)> items, JournalStreamWriter writer)
    {
        using var entry = writer.BeginEntry();
        using var session = sessionPool.GetSession();

        var payloadWriter = Writer.Create(entry.Writer, session);

        payloadWriter.WriteByte(VersionByte);
        payloadWriter.WriteVarUInt32((uint)CommandType.Snapshot);

        payloadWriter.WriteVarUInt64((ulong)windowSeconds);
        payloadWriter.WriteVarUInt32((uint)count);

        foreach (var (item, timestamp) in items)
        {
            codec.WriteField(ref payloadWriter, 0, typeof(T), item);
            payloadWriter.WriteVarUInt64((ulong)timestamp);
        }

        payloadWriter.Commit();
        entry.Commit();
    }

    public void Apply(JournalBufferReader input, IDurableTimeWindowBufferCommandHandler<T> handler)
    {
        ArgumentNullException.ThrowIfNull(handler);

        using var slice = input.Peek(input.Length);
        using var session = sessionPool.GetSession();

        var reader = Reader.Create(slice, session);
        var version = reader.ReadByte();

        if (version != VersionByte)
        {
            throw new NotSupportedException($"This command codec supports version {(uint)VersionByte} and not version {(uint)version}.");
        }

        var command = (CommandType)reader.ReadVarUInt32();

        switch (command)
        {
            case CommandType.Clear: handler.ApplyClear(); break;
            case CommandType.SetWindow: handler.ApplySetWindow((long)reader.ReadVarUInt64()); break;
            case CommandType.Enqueue: handler.ApplyEnqueue(ReadValue(ref reader), (long)reader.ReadVarUInt64()); break;
            case CommandType.Dequeue: handler.ApplyTryDequeue(); break;
            case CommandType.Snapshot:
                {
                    var windowSeconds = (long)reader.ReadVarUInt64();
                    var count = (int)reader.ReadVarUInt32();

                    handler.Reset(windowSeconds);

                    for (var i = 0; i < count; i++)
                    {
                        var item = ReadValue(ref reader);
                        var timestamp = (long)reader.ReadVarUInt64();

                        handler.ApplyEnqueue(item, timestamp);
                    }
                }
                break;
            default: Helpers.ThrowUnsupportedCommand(command); break;
        }

        Helpers.ThrowIfTrailingData(ref reader);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        T ReadValue<TInput>(ref Reader<TInput> reader)
        {
            var field = reader.ReadFieldHeader();
            return codec.ReadValue(ref reader, field);
        }
    }
}

[DebuggerDisplay("Count = {Count}, IsEmpty = {IsEmpty}, Window = {Window}")]
internal sealed class DurableTimeWindowBuffer<T> :
    IDurableTimeWindowBuffer<T>,
    IDurableTimeWindowBufferCommandHandler<T>,
    IJournaledState
{
    private JournalStreamWriter? _writer;
    private readonly TimeWindowBuffer<T> _buffer = new();
    private readonly TimeProvider _timeProvider;
    private readonly IDurableTimeWindowBufferCommandCodec<T> _codec;

    public DurableTimeWindowBuffer(
        [ServiceKey] string key, IJournaledStateManager manager,
        IOptions<JournaledStateManagerOptions> options, TimeProvider timeProvider,
        IServiceProvider serviceProvider)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        _timeProvider = timeProvider;
        _codec = Helpers.GetCodec<IDurableTimeWindowBufferCommandCodec<T>>(serviceProvider, options);
        manager.RegisterState(key, this);
    }

    public int Count => _buffer.Count;
    public bool IsEmpty => _buffer.IsEmpty;
    public TimeSpan Window => TimeSpan.FromSeconds(_buffer.WindowSeconds);

    #region IJournalState

    IJournaledState IJournaledState.DeepCopy() => throw new NotImplementedException();

    void IJournaledState.ReplayEntry(JournalEntry entry, JournalReplayContext context) =>
        context.GetRequiredCommandCodec(entry.FormatKey, _codec).Apply(entry.Reader, this);

    void IJournaledState.AppendEntries(JournalStreamWriter writer)
    {
        // We use a push model, and append entries upon modification.
    }

    void IJournaledState.AppendSnapshot(JournalStreamWriter snapshotWriter) => _codec.WriteSnapshot(_buffer.WindowSeconds, Count, _buffer.GetEntries(), snapshotWriter);

    void IJournaledState.Reset(JournalStreamWriter writer)
    {
        _buffer.Clear();
        _writer = writer;
    }

    #endregion

    #region IDurableTimeWindowBufferCommandHandler

    void IDurableTimeWindowBufferCommandHandler<T>.ApplySetWindow(long windowSeconds) => _buffer.SetWindow(windowSeconds, _timeProvider.GetUtcNow().ToUnixTimeSeconds());
    void IDurableTimeWindowBufferCommandHandler<T>.ApplyEnqueue(T item, long timestamp) => _buffer.Enqueue(item, timestamp);
    void IDurableTimeWindowBufferCommandHandler<T>.ApplyTryDequeue() => _buffer.TryDequeue(out _);
    void IDurableTimeWindowBufferCommandHandler<T>.ApplyClear() => _buffer.Clear();
    void IDurableTimeWindowBufferCommandHandler<T>.Reset(long windowSeconds)
    {
        _buffer.Clear();
        _buffer.SetWindow(windowSeconds, _timeProvider.GetUtcNow().ToUnixTimeSeconds());
    }

    #endregion

    public bool SetWindow(TimeSpan window)
    {
        var windowSeconds = (long)window.TotalSeconds;
        TimeWindowBuffer<T>.ThrowIfTooShortWindow(windowSeconds);

        if (windowSeconds == _buffer.WindowSeconds)
        {
            return false;
        }

        _codec.WriteSetWindow(windowSeconds, GetWriter());
        _buffer.SetWindow(windowSeconds, _timeProvider.GetUtcNow().ToUnixTimeSeconds());

        return true;
    }

    public void Enqueue(T item)
    {
        var timestamp = _timeProvider.GetUtcNow().ToUnixTimeSeconds();

        _codec.WriteEnqueue(item, timestamp, GetWriter());
        _buffer.Enqueue(item, timestamp);
    }

    public bool TryDequeue([MaybeNullWhen(false)] out T item)
    {
        if (_buffer.IsEmpty)
        {
            item = default;
            return false;
        }

        _codec.WriteDequeue(GetWriter());
        return _buffer.TryDequeue(out item);
    }

    public void Clear()
    {
        if (_buffer.IsEmpty)
        {
            return;
        }

        _codec.WriteClear(GetWriter());
        _buffer.Clear();
    }

    public bool Contains(T item) => _buffer.Contains(item);
    public int CopyTo(T[] array, int arrayIndex) => _buffer.CopyTo(array, arrayIndex);
    public int CopyTo(Span<T> destination) => _buffer.CopyTo(destination);

    public int DrainTo(T[] array, int arrayIndex)
    {
        ArgumentNullException.ThrowIfNull(array);
        return DrainTo(array.AsSpan(arrayIndex));
    }

    public int DrainTo(Span<T> destination)
    {
        var count = CopyTo(destination);
        if (count > 0)
        {
            Clear(); // We durably log this by means of clearing the buffer.
        }

        return count;
    }

    private JournalStreamWriter GetWriter()
    {
        Debug.Assert(_writer.HasValue);
        return _writer.Value;
    }

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    public IEnumerator<T> GetEnumerator() => _buffer.GetEnumerator();
}

internal sealed class DurableTimeWindowBufferDebugView<T>(DurableTimeWindowBuffer<T> buffer)
{
    [DebuggerBrowsable(DebuggerBrowsableState.RootHidden)]
    public T[] Items => [.. buffer];
}

internal sealed class TimeWindowBuffer<T> : IEnumerable<T>
{
    private readonly Queue<(T Item, long Timestamp)> _buffer = new();

    public int Count => _buffer.Count;
    public bool IsEmpty => _buffer.Count == 0;
    public long WindowSeconds { get; private set; } = 60 * 60; // We default to 1 hour

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void ThrowIfTooShortWindow(long windowSeconds)
    {
        if (windowSeconds < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(windowSeconds), "Window must be at least 1 second.");
        }
    }

    public bool Contains(T item)
    {
        var comparer = EqualityComparer<T>.Default;

        foreach (var (current, _) in _buffer)
        {
            if (comparer.Equals(current, item))
            {
                return true;
            }
        }

        return false;
    }

    public void Enqueue(T item, long timestamp)
    {
        _buffer.Enqueue((item, timestamp));
        PurgeOldItems(timestamp);
    }

    public bool TryDequeue([MaybeNullWhen(false)] out T item)
    {
        if (_buffer.TryDequeue(out var entry))
        {
            item = entry.Item;
            return true;
        }

        item = default;
        return false;
    }

    public bool SetWindow(long windowSeconds, long currentTimestamp)
    {
        ThrowIfTooShortWindow(windowSeconds);

        if (windowSeconds == WindowSeconds)
        {
            return false;
        }

        WindowSeconds = windowSeconds;
        PurgeOldItems(currentTimestamp);

        return true;
    }

    public int CopyTo(T[] array, int arrayIndex)
    {
        ArgumentNullException.ThrowIfNull(array);
        return CopyTo(array.AsSpan(arrayIndex));
    }

    public int CopyTo(Span<T> destination)
    {
        var count = Math.Min(destination.Length, _buffer.Count);
        if (count == 0)
        {
            return 0;
        }

        int i = 0;
        
        foreach (var (item, _) in _buffer)
        {
            if (i >= count)
            {
                break;
            }

            destination[i++] = item;
        }

        return count;
    }

    public bool Clear()
    {
        if (_buffer.Count == 0)
        {
            return false;
        }

        _buffer.Clear();

        return true;
    }

    private void PurgeOldItems(long currentTimestamp)
    {
        var evictionThreshold = currentTimestamp - WindowSeconds;

        while (_buffer.TryPeek(out var entry) && entry.Timestamp < evictionThreshold)
        {
            _buffer.Dequeue();
        }
    }

    /// <summary>
    /// Returns an enumerable of the raw entries, including timestamps.
    /// </summary>
    /// <remarks>Used for snapshotting.</remarks>
    public IEnumerable<(T Item, long TimestampSeconds)> GetEntries() => _buffer;

    public IEnumerator<T> GetEnumerator()
    {
        foreach (var (item, _) in _buffer)
        {
            yield return item;
        }
    }

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}