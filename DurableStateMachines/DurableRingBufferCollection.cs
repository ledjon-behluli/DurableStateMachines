using Microsoft.Extensions.DependencyInjection;
using System.Buffers;
using System.Collections;
using System.Diagnostics.CodeAnalysis;
using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace Ledjon.DurableStateMachines;

/// <summary>
/// Defines a durable collection of named, fixed-size circular buffers.
/// </summary>
/// <typeparam name="TKey">The type of the key used to identify each ring buffer.</typeparam>
/// <typeparam name="TValue">The type of elements in the ring buffers.</typeparam>
public interface IDurableRingBufferCollection<TKey, TValue> where TKey : notnull
{
    /// <summary>
    /// Gets the number of ring buffers in the collection.
    /// </summary>
    int Count { get; }

    /// <summary>
    /// Gets a reado-only collection containing the keys of the ring buffers.
    /// </summary>
    IReadOnlyCollection<TKey> Keys { get; }

    /// <summary>
    /// Ensures that a ring buffer associated with the specified key exists and is configured with the given capacity.
    /// </summary>
    /// <param name="key">The key of the ring buffer to ensure.</param>
    /// <param name="capacity">The desired capacity for the ring buffer.</param>
    /// <returns>A durable proxy to the ring buffer, which will have the specified capacity after this call.</returns>
    /// <remarks>
    /// <para>
    /// This method provides a convenient way to get a buffer and set its capacity in a single, atomic operation.
    /// If a buffer for the given <paramref name="key"/> does not exist, it will be created with the specified <paramref name="capacity"/>.
    /// If the buffer already exists, its capacity will be overwritten with the new value (if its different).
    /// </para>
    /// <para><strong>
    /// Decreasing the capacity on an existing buffer may result in data loss, if the number of items
    /// currently in the buffer exceeds the new capacity.
    /// </strong></para>
    /// </remarks>
    IDurableRingBuffer<TValue> EnsureBuffer(TKey key, int capacity);

    /// <summary>
    /// Determines whether the collection contains a ring buffer with the specified key.
    /// </summary>
    /// <param name="key">The key to locate.</param>
    /// <returns><c>true</c> if the collection contains a buffer with the key; otherwise, <c>false</c>.</returns>
    bool Contains(TKey key);

    /// <summary>
    /// Removes the ring buffer associated with the specified key from the collection.
    /// </summary>
    /// <param name="key">The key of the buffer to remove.</param>
    /// <returns><c>true</c> if the buffer was successfully found and removed; otherwise, <c>false</c>.</returns>
    bool Remove(TKey key);

    /// <summary>
    /// Removes all buffers from the collection.
    /// </summary>
    void Clear();
}

[DebuggerDisplay("Count = {Count}")]
[DebuggerTypeProxy(typeof(DurableRingBufferCollectionDebugView<,>))]
internal sealed class DurableRingBufferCollection<TKey, TValue> :
    IDurableRingBufferCollection<TKey, TValue>, IDurableStateMachine
        where TKey : notnull
{
    private const byte VersionByte = 0;

    private IStateMachineLogWriter? _storage;

    private readonly IFieldCodec<TKey> _keyCodec;
    private readonly IFieldCodec<TValue> _valueCodec;
    private readonly SerializerSessionPool _sessionPool;
    private readonly Dictionary<TKey, RingBufferProxy> _proxies = [];

    public DurableRingBufferCollection(
        [ServiceKey] string key, IStateMachineManager manager,
        IFieldCodec<TKey> keyCodec, IFieldCodec<TValue> valueCodec,
        SerializerSessionPool sessionPool)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);

        _keyCodec = keyCodec;
        _valueCodec = valueCodec;
        _sessionPool = sessionPool;

        manager.RegisterStateMachine(key, this);
    }

    public int Count => _proxies.Count;
    public IReadOnlyCollection<TKey> Keys => _proxies.Keys;

    public IDurableRingBuffer<TValue> EnsureBuffer(TKey key, int capacity)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(capacity, nameof(capacity));

        var proxy = GetOrCreateProxy(key);
        if (proxy.Capacity != capacity)
        {
            proxy.SetCapacity(capacity);
        }

        return proxy;
    }

    public bool Contains(TKey key) => _proxies.ContainsKey(key);

    public bool Remove(TKey key)
    {
        if (ApplyRemove(key))
        {
            GetStorage().AppendEntry(static (state, bufferWriter) =>
            {
                var (self, key) = state;

                using var session = self._sessionPool.GetSession();
                var writer = Writer.Create(bufferWriter, session);

                writer.WriteByte(VersionByte);
                writer.WriteVarUInt32((uint)CommandType.RemoveBuffer);

                self._keyCodec.WriteField(ref writer, 0, typeof(TKey), key);

                writer.Commit();
            }, (this, key));

            return true;
        }

        return false;
    }

    public void Clear()
    {
        if (_proxies.Count == 0)
        {
            return;
        }

        ApplyClear();
        GetStorage().AppendEntry(static (self, bufferWriter) =>
        {
            using var session = self._sessionPool.GetSession();

            var writer = Writer.Create(bufferWriter, session);

            writer.WriteByte(VersionByte);
            writer.WriteVarUInt32((uint)CommandType.ClearAll);

            writer.Commit();
        }, this);
    }

    void IDurableStateMachine.Reset(IStateMachineLogWriter storage)
    {
        ApplyClear();
        _storage = storage;
    }

    void IDurableStateMachine.AppendEntries(StateMachineStorageWriter writer)
    {
        // We use a push model, and append entries upon modification.
    }

    void IDurableStateMachine.Apply(ReadOnlySequence<byte> logEntry)
    {
        using var session = _sessionPool.GetSession();

        var reader = Reader.Create(logEntry, session);
        var version = reader.ReadByte();

        if (version != VersionByte)
        {
            throw new NotSupportedException($"This instance of {nameof(DurableRingBufferCollection<TKey, TValue>)} supports version {(uint)VersionByte} and not version {(uint)version}.");
        }

        var command = (CommandType)reader.ReadVarUInt32();

        switch (command)
        {
            case CommandType.Snapshot: ApplySnapshot(ref reader); break;
            case CommandType.ClearAll: ApplyClear(); break;
            case CommandType.ClearBuffer: _ = ApplyClearBuffer(ReadKey(ref reader)); break;
            case CommandType.RemoveBuffer: _ = ApplyRemove(ReadKey(ref reader)); break;
            case CommandType.SetCapacity: _ = ApplySetBufferCapacity(ReadKey(ref reader), (int)reader.ReadVarUInt32()); break;
            case CommandType.EnqueueItem: ApplyEnqueueBufferItem(ReadKey(ref reader), ReadValue(ref reader)); break;
            case CommandType.DequeueItem: _ = ApplyTryDequeueBufferItem(ReadKey(ref reader), out _); break;

            default: throw new NotSupportedException($"Command type {command} is not supported");
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        TKey ReadKey(ref Reader<ReadOnlySequenceInput> reader)
        {
            var field = reader.ReadFieldHeader();
            return _keyCodec.ReadValue(ref reader, field);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        TValue ReadValue(ref Reader<ReadOnlySequenceInput> reader)
        {
            var field = reader.ReadFieldHeader();
            return _valueCodec.ReadValue(ref reader, field);
        }

        void ApplySnapshot(ref Reader<ReadOnlySequenceInput> reader)
        {
            ApplyClear();

            var bufferCount = (int)reader.ReadVarUInt32();

            for (var i = 0; i < bufferCount; i++)
            {
                var key = ReadKey(ref reader);
                var capacity = (int)reader.ReadVarUInt32();
                var itemCount = (int)reader.ReadVarUInt32();

                var buffer = GetOrCreateProxy(key).Buffer;
                buffer.SetCapacity(capacity);

                for (var j = 0; j < itemCount; j++)
                {
                    buffer.Enqueue(ReadValue(ref reader));
                }
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

            writer.WriteVarUInt32((uint)self._proxies.Count);

            foreach (var (key, proxy) in self._proxies)
            {
                // First we write the key and the buffer-specific metadata.
                self._keyCodec.WriteField(ref writer, 0, typeof(TKey), key);

                var buffer = proxy.Buffer;

                writer.WriteVarUInt32((uint)buffer.Capacity);
                writer.WriteVarUInt32((uint)buffer.Count);

                // Then for each buffer we write its items.
                foreach (var item in buffer)
                {
                    self._valueCodec.WriteField(ref writer, 0, typeof(TValue), item);
                }
            }

            writer.Commit();
        }, this);
    }

    private bool SetBufferCapacity(TKey key, int capacity)
    {
        if (ApplySetBufferCapacity(key, capacity))
        {
            GetStorage().AppendEntry(static (state, bufferWriter) =>
            {
                var (self, key, capacity) = state;

                using var session = self._sessionPool.GetSession();

                var writer = Writer.Create(bufferWriter, session);

                writer.WriteByte(VersionByte);
                writer.WriteVarUInt32((uint)CommandType.SetCapacity);

                self._keyCodec.WriteField(ref writer, 0, typeof(TKey), key);
                writer.WriteVarUInt32((uint)capacity);

                writer.Commit();
            }, (this, key, capacity));

            return true;
        }

        return false;
    }

    private void EnqueueItem(TKey key, TValue item)
    {
        ApplyEnqueueBufferItem(key, item);
        GetStorage().AppendEntry(static (state, bufferWriter) =>
        {
            var (self, key, item) = state;

            using var session = self._sessionPool.GetSession();

            var writer = Writer.Create(bufferWriter, session);

            writer.WriteByte(VersionByte);
            writer.WriteVarUInt32((uint)CommandType.EnqueueItem);

            self._keyCodec.WriteField(ref writer, 0, typeof(TKey), key);
            self._valueCodec.WriteField(ref writer, 0, typeof(TValue), item);

            writer.Commit();
        }, (this, key, item));
    }

    private bool TryDequeueItem(TKey key, [MaybeNullWhen(false)] out TValue item)
    {
        if (ApplyTryDequeueBufferItem(key, out item))
        {
            GetStorage().AppendEntry(static (state, bufferWriter) =>
            {
                var (self, key) = state;

                using var session = self._sessionPool.GetSession();
                
                var writer = Writer.Create(bufferWriter, session);

                writer.WriteByte(VersionByte);
                writer.WriteVarUInt32((uint)CommandType.DequeueItem);

                self._keyCodec.WriteField(ref writer, 0, typeof(TKey), key);

                writer.Commit();
            }, (this, key));

            return true;
        }

        item = default;
        return false;
    }

    private void ClearBuffer(TKey key)
    {
        if (ApplyClearBuffer(key))
        {
            GetStorage().AppendEntry(static (state, bufferWriter) =>
            {
                var (self, key) = state;

                using var session = self._sessionPool.GetSession();

                var writer = Writer.Create(bufferWriter, session);

                writer.WriteByte(VersionByte);
                writer.WriteVarUInt32((uint)CommandType.ClearBuffer);

                self._keyCodec.WriteField(ref writer, 0, typeof(TKey), key);

                writer.Commit();
            }, (this, key));
        }
    }

    private bool ApplyRemove(TKey key) => _proxies.Remove(key, out _);
    private bool ApplySetBufferCapacity(TKey key, int capacity) => GetOrCreateProxy(key).Buffer.SetCapacity(capacity);
    private void ApplyEnqueueBufferItem(TKey key, TValue item) => GetOrCreateProxy(key).Buffer.Enqueue(item);
    private bool ApplyTryDequeueBufferItem(TKey key, out TValue item) => GetOrCreateProxy(key).Buffer.TryDequeue(out item!);
    private bool ApplyClearBuffer(TKey key) => GetOrCreateProxy(key).Buffer.Clear();
    private void ApplyClear() => _proxies.Clear();

    internal RingBufferProxy GetOrCreateProxy(TKey key)
    {
        if (!_proxies.TryGetValue(key, out var proxy))
        {
            proxy = new RingBufferProxy(key, this);
            _proxies[key] = proxy;
        }

        return proxy;
    }

    private IStateMachineLogWriter GetStorage()
    {
        Debug.Assert(_storage is not null);
        return _storage;
    }

    public IDurableStateMachine DeepCopy() => throw new NotImplementedException();

    private enum CommandType : uint
    {
        Snapshot = 0,
        ClearAll = 1,
        ClearBuffer = 2,
        RemoveBuffer = 3,
        SetCapacity = 4,
        EnqueueItem = 5,
        DequeueItem = 6

    }

    internal sealed class RingBufferProxy(
        TKey key, DurableRingBufferCollection<TKey, TValue> collection) :
            IDurableRingBuffer<TValue>
    {
        public RingBuffer<TValue> Buffer { get; } = new();

        public int Count => Buffer.Count;
        public int Capacity => Buffer.Capacity;
        public bool IsEmpty => Buffer.IsEmpty;
        public bool IsFull => Buffer.IsFull;

        public bool SetCapacity(int capacity) => collection.SetBufferCapacity(key, capacity);
        public void Enqueue(TValue item) => collection.EnqueueItem(key, item);
        public bool TryDequeue([MaybeNullWhen(false)] out TValue item) => collection.TryDequeueItem(key, out item);
        public void Clear() => collection.ClearBuffer(key);

        public int DrainTo(TValue[] array, int arrayIndex)
        {
            ArgumentNullException.ThrowIfNull(array);
            return DrainTo(array.AsSpan(arrayIndex));
        }

        public int DrainTo(Span<TValue> destination)
        {
            var count = Buffer.CopyTo(destination);
            if (count > 0)
            {
                Clear();
            }

            return count;
        }

        public int CopyTo(TValue[] array, int arrayIndex) => Buffer.CopyTo(array, arrayIndex);
        public int CopyTo(Span<TValue> destination) => Buffer.CopyTo(destination);

        public IEnumerator<TValue> GetEnumerator() => Buffer.GetEnumerator();
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }
}

internal sealed class DurableRingBufferCollectionDebugView<TKey, TValue>(
    DurableRingBufferCollection<TKey, TValue> collection) 
        where TKey : notnull
{
    [DebuggerBrowsable(DebuggerBrowsableState.RootHidden)]
    public KeyValuePair<TKey, TValue[]>[] Items
    {
        get
        {
            var i = 0;
            var result = new KeyValuePair<TKey, TValue[]>[collection.Count];

            foreach (var key in collection.Keys)
            {
                var proxy = collection.GetOrCreateProxy(key);
                result[i++] = new KeyValuePair<TKey, TValue[]>(key, [.. proxy]);
            }
            
            return result;
        }
    }
}