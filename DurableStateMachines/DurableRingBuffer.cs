using Microsoft.Extensions.DependencyInjection;
using System.Buffers;
using System.Collections;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace Ledjon.DurableStateMachines;

/// <summary>
/// Defines a durable, fixed-size circular buffer that stores the last N items in the order they were added.
/// When the buffer reaches its capacity, adding a new item will overwrite the oldest one.
/// </summary>
/// <typeparam name="T">Specifies the type of elements in the ring buffer.</typeparam>
public interface IDurableRingBuffer<T> : IEnumerable<T>, IReadOnlyCollection<T>
{
    /// <summary>
    /// Gets the maximum number of elements the buffer can hold.
    /// </summary>
    int Capacity { get; }

    /// <summary>
    /// Gets a value indicating whether the buffer is empty.
    /// </summary>
    bool IsEmpty { get; }

    /// <summary>
    /// Gets a value indicating whether the buffer is full.
    /// </summary>
    bool IsFull { get; }

    /// <summary>
    /// Sets the total capacity of the buffer. 
    /// If the new capacity is smaller than the current number of items, the oldest items are discarded.
    /// </summary>
    /// <param name="capacity">The desired new capacity.</param>
    /// <returns><c>true</c> if the internal capacity is different from <paramref name="capacity"/>, which in turn becomes the new capacity; otherwise, <c>false</c>.</returns>
    /// <exception cref="ArgumentOutOfRangeException">Thrown if capacity is zero or negative.</exception>
    bool SetCapacity(int capacity);

    /// <summary>
    /// Adds an item to the buffer. If the buffer is full, the oldest item is overwritten.
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

[DebuggerDisplay("Count = {Count}, Capacity = {Capacity}, IsEmpty = {IsEmpty}, IsFull = {IsFull}")]
[DebuggerTypeProxy(typeof(DurableRingBufferDebugView<>))]
internal sealed class DurableRingBuffer<T> : IDurableRingBuffer<T>, IDurableStateMachine
{
    private const byte VersionByte = 0;

    private IStateMachineLogWriter? _storage;

    private readonly RingBuffer<T> _buffer = new();
    private readonly SerializerSessionPool _sessionPool;
    private readonly IFieldCodec<T> _codec;

    public DurableRingBuffer(
        [ServiceKey] string key, IStateMachineManager manager,
        IFieldCodec<T> codec, SerializerSessionPool sessionPool)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);

        _codec = codec;
        _sessionPool = sessionPool;

        manager.RegisterStateMachine(key, this);
    }

    public int Count => _buffer.Count;
    public int Capacity => _buffer.Capacity;
    public bool IsEmpty => _buffer.IsEmpty;
    public bool IsFull => _buffer.IsFull;
    
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
            throw new NotSupportedException($"This instance of {nameof(DurableRingBuffer<T>)} supports version {(uint)VersionByte} and not version {(uint)version}.");
        }

        var command = (CommandType)reader.ReadVarUInt32();

        switch (command)
        {
            case CommandType.SetCapacity: _ = ApplySetCapacity((int)reader.ReadVarUInt32()); break;
            case CommandType.Enqueue: ApplyEnqueue(ReadValue(ref reader)); break;
            case CommandType.Dequeue: _ = ApplyTryDequeue(out _); break;
            case CommandType.Clear: _ = ApplyClear(); break;
            case CommandType.Snapshot: ApplySnapshot(ref reader); break;
            default: throw new NotSupportedException($"Command type {command} is not supported");
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        T ReadValue(ref Reader<ReadOnlySequenceInput> reader)
        {
            var field = reader.ReadFieldHeader();
            return _codec.ReadValue(ref reader, field);
        }

        void ApplySnapshot(ref Reader<ReadOnlySequenceInput> reader)
        {
            var count = (int)reader.ReadVarUInt32();
            var capacity = (int)reader.ReadVarUInt32();

            //  Since we support a dynamic capacity, we restore the buffer to the capacity it had when the snapshot was taken.   
            ApplySetCapacity(capacity); // This implicitly resets the buffer to the correct size.

            for (var i = 0; i < count; i++)
            {
                ApplyEnqueue(ReadValue(ref reader));
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
            
            writer.WriteVarUInt32((uint)self.Count);
            writer.WriteVarUInt32((uint)self.Capacity);

            foreach (var item in self)
            {
                self._codec.WriteField(ref writer, 0, typeof(T), item);
            }

            writer.Commit();
        }, this);
    }

    public bool SetCapacity(int capacity)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(capacity, nameof(capacity));

        if (ApplySetCapacity(capacity))
        {
            GetStorage().AppendEntry(static (state, bufferWriter) =>
            {
                var (self, capacity) = state;
               
                using var session = self._sessionPool.GetSession();
                
                var writer = Writer.Create(bufferWriter, session);

                writer.WriteByte(VersionByte);
                writer.WriteVarUInt32((uint)CommandType.SetCapacity);
                
                writer.WriteVarUInt32((uint)capacity);
                
                writer.Commit();
            }, (this, capacity));

            return true;
        }

        return false;
    }

    public void Enqueue(T item)
    {
        ApplyEnqueue(item);
        GetStorage().AppendEntry(static (state, bufferWriter) =>
        {
            var (self, item) = state;

            using var session = self._sessionPool.GetSession();
            
            var writer = Writer.Create(bufferWriter, session);

            writer.WriteByte(VersionByte);
            writer.WriteVarUInt32((uint)CommandType.Enqueue);
            
            self._codec.WriteField(ref writer, 0, typeof(T), item);
            
            writer.Commit();
        }, (this, item));
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

    private bool ApplySetCapacity(int capacity) => _buffer.SetCapacity(capacity);
    private void ApplyEnqueue(T item) => _buffer.Enqueue(item);
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
        SetCapacity = 2,
        Enqueue = 3,
        Dequeue = 4
    }
}

internal sealed class DurableRingBufferDebugView<T>(DurableRingBuffer<T> buffer)
{
    [DebuggerBrowsable(DebuggerBrowsableState.RootHidden)]
    public T[] Items => [.. buffer];
}

internal sealed class RingBuffer<T> : IEnumerable<T>
{
    private int _head = 0;  // Points to the index where the next item will be written.
    private int _tail = 0;  // Points to the index where the next item will be read.
    private int _count = 0;

    private T[] _buffer = new T[1]; // Users are supposed to set their desired capacity!

    public int Capacity => _buffer.Length;
    public int Count => _count;
    public bool IsEmpty => _count == 0;
    public bool IsFull => _count == Capacity;

    public bool Contains(T item)
    {
        if (IsEmpty)
        {
            return false;
        }

        var span = new ReadOnlySpan<T>(_buffer);

        // Case 1: The items are in a single contiguous block: [_, _, T, T, H, _]
        if (_tail < _head)
        {
            return Exists(span, _tail, _count, item);
        }

        // Case 2: The items are wrapped around the end of the buffer: [T, T, H, _, T]
        // This also handles the case where the buffer is full (_head == _tail).

        // We search the first segment (from the tail to the end of the buffer).
        if (Exists(span, _tail, Capacity - _tail, item))
        {
            return true;
        }

        // Otherwise, we search the second segment (from the start of the buffer up to the head).
        return Exists(span, 0, _head, item);

        static bool Exists(ReadOnlySpan<T> buffer, int index, int segmentLength, T item)
        {
            var comparer = EqualityComparer<T>.Default;
            var end = index + segmentLength;

            for (int i = index; i < end; i++)
            {
                if (comparer.Equals(buffer[i], item))
                {
                    return true;
                }
            }

            return false;
        }
    }

    public void Enqueue(T item)
    {
        _buffer[_head] = item;
        _head = (_head + 1) % Capacity;

        if (IsFull)
        {
            _tail = (_tail + 1) % Capacity; // This means that the head has overwritten the tail.
        }
        else
        {
            _count++;
        }
    }

    public bool TryDequeue([MaybeNullWhen(false)] out T item)
    {
        if (IsEmpty)
        {
            item = default;
            return false;
        }

        item = _buffer[_tail];

        if (RuntimeHelpers.IsReferenceOrContainsReferences<T>())
        {
            _buffer[_tail] = default!; // To avoid a potential memory leak.
        }

        _tail = (_tail + 1) % Capacity;
        _count--;

        return true;
    }

    public bool SetCapacity(int capacity)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(capacity, nameof(capacity));

        if (capacity == Capacity)
        {
            return false;
        }

        var newBuffer = new T[capacity];
        var itemsToKeep = Math.Min(_count, capacity);

        if (itemsToKeep > 0)
        {
            // We calculate the start index of the newest items by skipping the oldest ones.

            var oldestItemsToSkip = _count - itemsToKeep;
            var startOfNewestItemsIndex = (_tail + oldestItemsToSkip) % Capacity;

            var source = _buffer.AsSpan();
            var destination = newBuffer.AsSpan();

            if (startOfNewestItemsIndex < _head || _head == 0)
            {
                // The newest items are in a single contiguous block (in the old buffer).
                source.Slice(startOfNewestItemsIndex, itemsToKeep).CopyTo(destination);
            }
            else
            {
                // The newest items are split into two segments.
                // e.g., [4, 5, 1, 2, 3] where tail=2, head=2, count=5. 
                // If we want to keep 3 items (3 , 4, 5), start index is (2 + 5 - 3) % 5 = 4.
                // Items are at index 4, and then wraps to 0 and 1.

                // First segment: from the calculated start to the end of the old buffer.
                var rightSegmentLength = Capacity - startOfNewestItemsIndex;
                source.Slice(startOfNewestItemsIndex, rightSegmentLength).CopyTo(destination);

                // Second segment: from the beginning of the old buffer.
                var leftSegmentLength = itemsToKeep - rightSegmentLength;
                source.Slice(0, leftSegmentLength).CopyTo(destination.Slice(rightSegmentLength));
            }
        }

        _buffer = newBuffer;
        _count = itemsToKeep;
        _tail = 0;
        _head = (itemsToKeep == capacity) ? 0 : itemsToKeep;

        return true;
    }

    public int CopyTo(T[] array, int arrayIndex)
    {
        ArgumentNullException.ThrowIfNull(array);
        return CopyTo(array.AsSpan(arrayIndex));
    }

    public int CopyTo(Span<T> destination)
    {
        var count = Math.Min(destination.Length, _count);
        if (count == 0)
        {
            return 0;
        }

        var source = new ReadOnlySpan<T>(_buffer);

        // The items can be either in one contiguous or two disjoint segments.
        if (_tail < _head)
        {
            // The items are in a contiguous segment: [_, _, T..H, _, _]
            source.Slice(_tail, count).CopyTo(destination);
        }
        else
        {
            // The items are in two segments: [H.., _, ..T], in other words they are wrapped around the end.
            var rightSegment = source.Slice(_tail, Capacity - _tail);
            var leftSegment = source.Slice(0, _head);

            if (rightSegment.Length >= count)
            {
                rightSegment.Slice(0, count).CopyTo(destination);
            }
            else
            {
                rightSegment.CopyTo(destination);
                leftSegment.Slice(0, count - rightSegment.Length).CopyTo(destination.Slice(rightSegment.Length));
            }
        }
        return count;
    }

    public bool Clear()
    {
        if (_count == 0)
        {
            return false;
        }

        if (RuntimeHelpers.IsReferenceOrContainsReferences<T>())
        {
            // To avoid a potential memory leak, we clear the array, which should tell the
            // GC we no longer are clinging to the references of the items we hold in the buffer.
            // This is only valid for reference types or structs holding onto other reference types.

            Array.Clear(_buffer, 0, Capacity);
        }

        _head = 0;
        _tail = 0;
        _count = 0;

        return true;
    }

    public Enumerator GetEnumerator() => new(this);

    IEnumerator<T> IEnumerable<T>.GetEnumerator() => GetEnumerator();
    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    public struct Enumerator(RingBuffer<T> buffer) : IEnumerator<T>
    {
        private int _index = -1;
        [AllowNull] private T _current = default;

        public readonly T Current => _current;
        readonly object? IEnumerator.Current => Current;

        readonly void IDisposable.Dispose() { }

        public void Reset()
        {
            _index = -1;
            _current = default;
        }

        public bool MoveNext()
        {
            if (buffer._buffer is null || _index >= buffer.Count - 1)
            {
                _index = buffer.Count;
                _current = default;

                return false;
            }

            _index++;

            var actualIndex = (buffer._tail + _index) % buffer.Capacity;
            
            _current = buffer._buffer[actualIndex];

            return true;
        }
    }
}