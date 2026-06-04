using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using System.Collections;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
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
    /// If the buffer already exists, its capacity will be overwritten with the new value (if different).
    /// </para>
    /// <para><strong>Decreasing the capacity on an existing buffer may result in data loss, if the number of items currently in the buffer exceeds the new capacity.</strong></para>
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

/// <summary>
/// Receives decoded durable ring buffer collection commands from a codec implementation.
/// </summary>
public interface IDurableRingBufferCollectionCommandHandler<TKey, TValue>
{
    /// <summary>Applies a set capacity command.</summary>
    void ApplySetCapacity(TKey key, int capacity);

    /// <summary>Applies an enqueue item command.</summary>
    void ApplyEnqueueItem(TKey key, TValue item);

    /// <summary>Applies a try dequeue item command.</summary>
    void ApplyTryDequeueItem(TKey key);

    /// <summary>Applies a clear buffer command.</summary>
    void ApplyClearBuffer(TKey key);

    /// <summary>Applies a remove buffer command.</summary>
    void ApplyRemoveBuffer(TKey key);

    /// <summary>Applies a clear all command.</summary>
    void ApplyClearAll();

    /// <summary>Resets the receiver before applying replacement entries.</summary>
    void Reset();
}

/// <summary>
/// Serializes one durable ring buffer collection command and applies one decoded command.
/// </summary>
public interface IDurableRingBufferCollectionCommandCodec<TKey, TValue>
{
    /// <summary>Writes a set capacity command.</summary>
    void WriteSetCapacity(TKey key, int capacity, JournalStreamWriter writer);

    /// <summary>Writes an enqueue item command.</summary>
    void WriteEnqueueItem(TKey key, TValue item, JournalStreamWriter writer);

    /// <summary>Writes a dequeue item command.</summary>
    void WriteDequeueItem(TKey key, JournalStreamWriter writer);

    /// <summary>Writes a clear buffer command.</summary>
    void WriteClearBuffer(TKey key, JournalStreamWriter writer);

    /// <summary>Writes a remove buffer command.</summary>
    void WriteRemoveBuffer(TKey key, JournalStreamWriter writer);

    /// <summary>Writes a clear all command.</summary>
    void WriteClearAll(JournalStreamWriter writer);

    /// <summary>Writes a snapshot command, deriving the item count from <paramref name="buffers"/>.</summary>
    void WriteSnapshot(int bufferCount, IEnumerable<(TKey Key, int Capacity, int Count, IEnumerable<TValue> Items)> buffers, JournalStreamWriter writer);

    /// <summary>Reads one encoded command and applies it to <paramref name="handler"/>.</summary>
    void Apply(JournalBufferReader input, IDurableRingBufferCollectionCommandHandler<TKey, TValue> handler);
}

internal sealed class DurableRingBufferCollectionCommandBinaryCodec<TKey, TValue>(
    IFieldCodec<TKey> keyCodec, IFieldCodec<TValue> valueCodec, SerializerSessionPool sessionPool)
        : IDurableRingBufferCollectionCommandCodec<TKey, TValue>
{
    private const byte VersionByte = 0;

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

    public void WriteSetCapacity(TKey key, int capacity, JournalStreamWriter writer)
    {
        using var entry = writer.BeginEntry();
        using var session = sessionPool.GetSession();

        var payloadWriter = Writer.Create(entry.Writer, session);

        payloadWriter.WriteByte(VersionByte);
        payloadWriter.WriteVarUInt32((uint)CommandType.SetCapacity);

        keyCodec.WriteField(ref payloadWriter, 0, typeof(TKey), key);
        payloadWriter.WriteVarUInt32((uint)capacity);

        payloadWriter.Commit();
        entry.Commit();
    }

    public void WriteEnqueueItem(TKey key, TValue item, JournalStreamWriter writer)
    {
        using var entry = writer.BeginEntry();
        using var session = sessionPool.GetSession();

        var payloadWriter = Writer.Create(entry.Writer, session);

        payloadWriter.WriteByte(VersionByte);
        payloadWriter.WriteVarUInt32((uint)CommandType.EnqueueItem);

        keyCodec.WriteField(ref payloadWriter, 0, typeof(TKey), key);
        valueCodec.WriteField(ref payloadWriter, 0, typeof(TValue), item);

        payloadWriter.Commit();
        entry.Commit();
    }

    public void WriteDequeueItem(TKey key, JournalStreamWriter writer)
    {
        using var entry = writer.BeginEntry();
        using var session = sessionPool.GetSession();

        var payloadWriter = Writer.Create(entry.Writer, session);

        payloadWriter.WriteByte(VersionByte);
        payloadWriter.WriteVarUInt32((uint)CommandType.DequeueItem);

        keyCodec.WriteField(ref payloadWriter, 0, typeof(TKey), key);

        payloadWriter.Commit();
        entry.Commit();
    }

    public void WriteClearBuffer(TKey key, JournalStreamWriter writer)
    {
        using var entry = writer.BeginEntry();
        using var session = sessionPool.GetSession();

        var payloadWriter = Writer.Create(entry.Writer, session);

        payloadWriter.WriteByte(VersionByte);
        payloadWriter.WriteVarUInt32((uint)CommandType.ClearBuffer);

        keyCodec.WriteField(ref payloadWriter, 0, typeof(TKey), key);

        payloadWriter.Commit();
        entry.Commit();
    }

    public void WriteRemoveBuffer(TKey key, JournalStreamWriter writer)
    {
        using var entry = writer.BeginEntry();
        using var session = sessionPool.GetSession();

        var payloadWriter = Writer.Create(entry.Writer, session);

        payloadWriter.WriteByte(VersionByte);
        payloadWriter.WriteVarUInt32((uint)CommandType.RemoveBuffer);

        keyCodec.WriteField(ref payloadWriter, 0, typeof(TKey), key);

        payloadWriter.Commit();
        entry.Commit();
    }

    public void WriteClearAll(JournalStreamWriter writer)
    {
        using var entry = writer.BeginEntry();
        using var session = sessionPool.GetSession();

        var payloadWriter = Writer.Create(entry.Writer, session);

        payloadWriter.WriteByte(VersionByte);
        payloadWriter.WriteVarUInt32((uint)CommandType.ClearAll);

        payloadWriter.Commit();
        entry.Commit();
    }

    public void WriteSnapshot(int bufferCount, IEnumerable<(TKey Key, int Capacity, int Count, IEnumerable<TValue> Items)> buffers, JournalStreamWriter writer)
    {
        using var entry = writer.BeginEntry();
        using var session = sessionPool.GetSession();

        var payloadWriter = Writer.Create(entry.Writer, session);

        payloadWriter.WriteByte(VersionByte);
        payloadWriter.WriteVarUInt32((uint)CommandType.Snapshot);

        payloadWriter.WriteVarUInt32((uint)bufferCount);

        foreach (var (key, capacity, count, items) in buffers)
        {
            // First we write the key and the buffer-specific metadata.
            keyCodec.WriteField(ref payloadWriter, 0, typeof(TKey), key);

            payloadWriter.WriteVarUInt32((uint)capacity);
            payloadWriter.WriteVarUInt32((uint)count);

            // Then for the current buffer we write all its items.
            foreach (var item in items)
            {
                valueCodec.WriteField(ref payloadWriter, 0, typeof(TValue), item);
            }
        }

        payloadWriter.Commit();
        entry.Commit();
    }

    public void Apply(JournalBufferReader input, IDurableRingBufferCollectionCommandHandler<TKey, TValue> handler)
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
            case CommandType.ClearAll: handler.ApplyClearAll(); break;
            case CommandType.ClearBuffer: handler.ApplyClearBuffer(ReadKey(ref reader)); break;
            case CommandType.RemoveBuffer: handler.ApplyRemoveBuffer(ReadKey(ref reader)); break;
            case CommandType.SetCapacity: handler.ApplySetCapacity(ReadKey(ref reader), (int)reader.ReadVarUInt32()); break;
            case CommandType.EnqueueItem: handler.ApplyEnqueueItem(ReadKey(ref reader), ReadValue(ref reader)); break;
            case CommandType.DequeueItem: handler.ApplyTryDequeueItem(ReadKey(ref reader)); break;
            case CommandType.Snapshot:
                {
                    var bufferCount = (int)reader.ReadVarUInt32();
                    handler.Reset();

                    for (var i = 0; i < bufferCount; i++)
                    {
                        var key = ReadKey(ref reader);
                        var capacity = (int)reader.ReadVarUInt32();
                        var itemCount = (int)reader.ReadVarUInt32();

                        handler.ApplySetCapacity(key, capacity);

                        for (var j = 0; j < itemCount; j++)
                        {
                            handler.ApplyEnqueueItem(key, ReadValue(ref reader));
                        }
                    }
                }
                break;
            default: Helpers.ThrowUnsupportedCommand(command); break;
        }

        Helpers.ThrowIfTrailingData(ref reader);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        TKey ReadKey<TInput>(ref Reader<TInput> reader)
        {
            var field = reader.ReadFieldHeader();
            return keyCodec.ReadValue(ref reader, field);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        TValue ReadValue<TInput>(ref Reader<TInput> reader)
        {
            var field = reader.ReadFieldHeader();
            return valueCodec.ReadValue(ref reader, field);
        }
    }
}

[DebuggerDisplay("Count = {Count}")]
internal sealed class DurableRingBufferCollection<TKey, TValue> :
    IDurableRingBufferCollection<TKey, TValue>,
    IDurableRingBufferCollectionCommandHandler<TKey, TValue>,
    IJournaledState
        where TKey : notnull
{
    private JournalStreamWriter? _writer;
    private readonly Dictionary<TKey, RingBufferProxy> _proxies = [];
    private readonly IDurableRingBufferCollectionCommandCodec<TKey, TValue> _codec;

    public DurableRingBufferCollection(
        [ServiceKey] string key, IJournaledStateManager manager,
        IOptions<JournaledStateManagerOptions> options, IServiceProvider serviceProvider)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        _codec = Helpers.GetCodec<IDurableRingBufferCollectionCommandCodec<TKey, TValue>>(serviceProvider, options);
        manager.RegisterState(key, this);
    }

    public int Count => _proxies.Count;
    public IReadOnlyCollection<TKey> Keys => _proxies.Keys;

    #region IJournalState

    IJournaledState IJournaledState.DeepCopy() => throw new NotImplementedException();

    void IJournaledState.ReplayEntry(JournalEntry entry, JournalReplayContext context) =>
        context.GetRequiredCommandCodec(entry.FormatKey, _codec).Apply(entry.Reader, this);

    void IJournaledState.AppendEntries(JournalStreamWriter writer)
    {
        // We use a push model, and append entries upon modification.
    }

    void IJournaledState.AppendSnapshot(JournalStreamWriter snapshotWriter) => _codec.WriteSnapshot(_proxies.Count, GetSnapshotData(), snapshotWriter);

    void IJournaledState.Reset(JournalStreamWriter writer)
    {
        _proxies.Clear();
        _writer = writer;
    }

    #endregion

    #region IDurableRingBufferCollectionCommandHandler

    void IDurableRingBufferCollectionCommandHandler<TKey, TValue>.ApplySetCapacity(TKey key, int capacity) => GetOrCreateProxy(key).Buffer.SetCapacity(capacity);
    void IDurableRingBufferCollectionCommandHandler<TKey, TValue>.ApplyEnqueueItem(TKey key, TValue item) => GetOrCreateProxy(key).Buffer.Enqueue(item);
    void IDurableRingBufferCollectionCommandHandler<TKey, TValue>.ApplyTryDequeueItem(TKey key) => GetOrCreateProxy(key).Buffer.TryDequeue(out _);
    void IDurableRingBufferCollectionCommandHandler<TKey, TValue>.ApplyClearBuffer(TKey key) => GetOrCreateProxy(key).Buffer.Clear();
    void IDurableRingBufferCollectionCommandHandler<TKey, TValue>.ApplyRemoveBuffer(TKey key) => _proxies.Remove(key);
    void IDurableRingBufferCollectionCommandHandler<TKey, TValue>.ApplyClearAll() => _proxies.Clear();
    void IDurableRingBufferCollectionCommandHandler<TKey, TValue>.Reset() => _proxies.Clear();

    #endregion

    public IDurableRingBuffer<TValue> EnsureBuffer(TKey key, int capacity)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(capacity, nameof(capacity));

        var proxy = GetOrCreateProxy(key);
        if (proxy.Capacity != capacity)
        {
            _codec.WriteSetCapacity(key, capacity, GetWriter());
            proxy.Buffer.SetCapacity(capacity);
        }

        return proxy;
    }

    public bool Contains(TKey key) => _proxies.ContainsKey(key);

    public bool Remove(TKey key)
    {
        if (!_proxies.ContainsKey(key))
        {
            return false;
        }

        _codec.WriteRemoveBuffer(key, GetWriter());
        _proxies.Remove(key);

        return true;
    }

    public void Clear()
    {
        if (_proxies.Count == 0)
        {
            return;
        }

        _codec.WriteClearAll(GetWriter());
        _proxies.Clear();
    }

    internal void SetBufferCapacity(TKey key, int capacity)
    {
        var proxy = GetOrCreateProxy(key);
        if (proxy.Buffer.Capacity == capacity)
        {
            return;
        }

        _codec.WriteSetCapacity(key, capacity, GetWriter());
        proxy.Buffer.SetCapacity(capacity);
    }

    internal void EnqueueItem(TKey key, TValue item)
    {
        _codec.WriteEnqueueItem(key, item, GetWriter());
        GetOrCreateProxy(key).Buffer.Enqueue(item);
    }

    internal bool TryDequeueItem(TKey key, [MaybeNullWhen(false)] out TValue item)
    {
        if (!_proxies.TryGetValue(key, out var proxy) || proxy.Buffer.IsEmpty)
        {
            item = default;
            return false;
        }

        _codec.WriteDequeueItem(key, GetWriter());
        return proxy.Buffer.TryDequeue(out item);
    }

    internal void ClearBuffer(TKey key)
    {
        if (!_proxies.TryGetValue(key, out var proxy) || proxy.Buffer.IsEmpty)
        {
            return;
        }

        _codec.WriteClearBuffer(key, GetWriter());
        proxy.Buffer.Clear();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal RingBufferProxy GetOrCreateProxy(TKey key)
    {
        if (!_proxies.TryGetValue(key, out var proxy))
        {
            proxy = new RingBufferProxy(key, this);
            _proxies[key] = proxy;
        }

        return proxy;
    }

    private IEnumerable<(TKey Key, int Capacity, int Count, IEnumerable<TValue> Items)> GetSnapshotData()
    {
        foreach (var (key, proxy) in _proxies)
        {
            yield return (key, proxy.Buffer.Capacity, proxy.Buffer.Count, proxy.Buffer);
        }
    }

    private JournalStreamWriter GetWriter()
    {
        Debug.Assert(_writer.HasValue);
        return _writer.Value;
    }

    internal sealed class RingBufferProxy(TKey key, DurableRingBufferCollection<TKey, TValue> collection) : IDurableRingBuffer<TValue>
    {
        public RingBuffer<TValue> Buffer { get; } = new();

        public int Count => Buffer.Count;
        public int Capacity => Buffer.Capacity;
        public bool IsEmpty => Buffer.IsEmpty;
        public bool IsFull => Buffer.IsFull;

        public bool Contains(TValue item) => Buffer.Contains(item);
        public bool SetCapacity(int capacity)
        {
            ArgumentOutOfRangeException.ThrowIfNegativeOrZero(capacity, nameof(capacity));
            if (Buffer.Capacity == capacity)
            {
                return false;
            }
            collection.SetBufferCapacity(key, capacity);
            return true;
        }

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