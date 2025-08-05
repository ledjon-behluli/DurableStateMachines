using System.Buffers;
using System.Collections;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using Microsoft.Extensions.DependencyInjection;

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

[DebuggerDisplay("Count = {Count}, IsEmpty = {IsEmpty}, Window = {Window}")]
[DebuggerTypeProxy(typeof(DurableTimeWindowBufferDebugView<>))]
internal sealed class DurableTimeWindowBuffer<T> : IDurableTimeWindowBuffer<T>, IDurableStateMachine
{
    private const byte VersionByte = 0;

    private IStateMachineLogWriter? _storage;

    private readonly TimeWindowBuffer<T> _buffer = new();
    private readonly SerializerSessionPool _sessionPool;
    private readonly TimeProvider _timeProvider;
    private readonly IFieldCodec<T> _codec;

    public DurableTimeWindowBuffer(
        [ServiceKey] string key, IStateMachineManager manager,
        IFieldCodec<T> codec, SerializerSessionPool sessionPool, TimeProvider timeProvider)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);

        _codec = codec;
        _sessionPool = sessionPool;
        _timeProvider = timeProvider;

        manager.RegisterStateMachine(key, this);
    }

    public int Count => _buffer.Count;
    public bool IsEmpty => _buffer.IsEmpty;
    public TimeSpan Window => TimeSpan.FromSeconds(_buffer.WindowSeconds);

    void IDurableStateMachine.AppendEntries(StateMachineStorageWriter writer)
    {
        // We use a push model, and append entries upon modification.
    }

    void IDurableStateMachine.Reset(IStateMachineLogWriter storage)
    {
        ApplyClear();
        _storage = storage;
    }

    void IDurableStateMachine.Apply(ReadOnlySequence<byte> logEntry)
    {
        using var session = _sessionPool.GetSession();

        var reader = Reader.Create(logEntry, session);
        var version = reader.ReadByte();

        if (version != VersionByte)
        {
            throw new NotSupportedException($"This instance of {nameof(DurableTimeWindowBuffer<T>)} supports version {(uint)VersionByte} and not version {(uint)version}.");
        }

        var command = (CommandType)reader.ReadVarUInt32();

        switch (command)
        {
            case CommandType.SetWindow: _ = ApplySetWindow((long)reader.ReadVarUInt64()); break;
            case CommandType.Enqueue: ApplyEnqueue(ReadValue(ref reader), (long)reader.ReadVarUInt64()); break;
            case CommandType.Dequeue: _ = ApplyTryDequeue(out _); break;
            case CommandType.Clear: _ = ApplyClear(); break;
            case CommandType.Snapshot: ApplySnapshot(ref reader); break;
            default: throw new NotSupportedException($"Command type is not supported");
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        T ReadValue(ref Reader<ReadOnlySequenceInput> reader)
        {
            var field = reader.ReadFieldHeader();
            return _codec.ReadValue(ref reader, field);
        }

        void ApplySnapshot(ref Reader<ReadOnlySequenceInput> reader)
        {
            var windowSeconds = (long)reader.ReadVarUInt64();
            var count = (int)reader.ReadVarUInt32();

            ApplySetWindow(windowSeconds);

            for (var i = 0; i < count; i++)
            {
                var item = ReadValue(ref reader);
                var timestamp = (long)reader.ReadVarUInt64();

                ApplyEnqueue(item, timestamp);
            }
        }
    }

    void IDurableStateMachine.AppendSnapshot(StateMachineStorageWriter writer)
    {
        writer.AppendEntry(static (self, bufferWriter) =>
        {
            using var session = self._sessionPool.GetSession();

            var writer = Writer.Create(bufferWriter, session);

            writer.WriteByte(VersionByte);
            writer.WriteVarUInt32((uint)CommandType.Snapshot);

            writer.WriteVarUInt64((ulong)self._buffer.WindowSeconds);
            writer.WriteVarUInt32((uint)self.Count);

            foreach (var (item, timestamp) in self._buffer.GetEntries())
            {
                self._codec.WriteField(ref writer, 0, typeof(T), item);
                writer.WriteVarUInt64((ulong)timestamp);
            }

            writer.Commit();
        }, this);
    }

    public bool SetWindow(TimeSpan window)
    {
        var windowSeconds = (long)window.TotalSeconds;

        TimeWindowBuffer<T>.ThrowIfTooShortWindow(windowSeconds);

        if (ApplySetWindow(windowSeconds))
        {
            GetStorage().AppendEntry(static (state, bufferWriter) =>
            {
                var (self, windowSeconds) = state;

                using var session = self._sessionPool.GetSession();

                var writer = Writer.Create(bufferWriter, session);

                writer.WriteByte(VersionByte);
                writer.WriteVarUInt32((uint)CommandType.SetWindow);

                writer.WriteVarUInt64((ulong)windowSeconds);

                writer.Commit();
            }, (this, windowSeconds));

            return true;
        }

        return false;
    }

    public void Enqueue(T item)
    {
        var timestamp = _timeProvider.GetUtcNow().ToUnixTimeSeconds();

        ApplyEnqueue(item, timestamp);
        GetStorage().AppendEntry(static (state, bufferWriter) =>
        {
            var (self, item, timestamp) = state;

            using var session = self._sessionPool.GetSession();

            var writer = Writer.Create(bufferWriter, session);

            writer.WriteByte(VersionByte);
            writer.WriteVarUInt32((uint)CommandType.Enqueue);

            self._codec.WriteField(ref writer, 0, typeof(T), item);
            writer.WriteVarUInt64((ulong)timestamp);

            writer.Commit();
        }, (this, item, timestamp));
    }

    public bool TryDequeue([MaybeNullWhen(false)] out T item)
    {
        if (ApplyTryDequeue(out item))
        {
            GetStorage().AppendEntry(static (self, bufferWriter) =>
            {
                using var session = self._sessionPool.GetSession();

                var writer = Writer.Create(bufferWriter, session);

                writer.WriteByte(VersionByte);
                writer.WriteVarUInt32((uint)CommandType.Dequeue);

                writer.Commit();
            }, this);

            return true;
        }

        return false;
    }

    public void Clear()
    {
        if (ApplyClear())
        {
            GetStorage().AppendEntry(static (self, bufferWriter) =>
            {
                using var session = self._sessionPool.GetSession();

                var writer = Writer.Create(bufferWriter, session);

                writer.WriteByte(VersionByte);
                writer.WriteVarUInt32((uint)CommandType.Clear);

                writer.Commit();
            }, this);
        }
    }

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

    private bool ApplySetWindow(long windowSeconds) => _buffer.SetWindow(windowSeconds, _timeProvider.GetUtcNow().ToUnixTimeSeconds());
    private void ApplyEnqueue(T item, long timestamp) => _buffer.Enqueue(item, timestamp);
    private bool ApplyTryDequeue(out T item) => _buffer.TryDequeue(out item!);
    private bool ApplyClear() => _buffer.Clear();

    private IStateMachineLogWriter GetStorage()
    {
        Debug.Assert(_storage is not null);
        return _storage;
    }

    public IDurableStateMachine DeepCopy() => throw new NotImplementedException();

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    public IEnumerator<T> GetEnumerator() => _buffer.GetEnumerator();

    public enum CommandType : uint
    {
        Clear = 0,
        Snapshot = 1,
        SetWindow = 2,
        Enqueue = 3,
        Dequeue = 4
    }
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