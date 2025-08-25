using Microsoft.Extensions.DependencyInjection;
using System.Buffers;
using System.Collections;
using System.Diagnostics.CodeAnalysis;
using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace Ledjon.DurableStateMachines;

/// <summary>
/// Defines a durable collection of named, time-based window buffers.
/// </summary>
/// <typeparam name="TKey">The type of the key used to identify each window buffer.</typeparam>
/// <typeparam name="TValue">The type of elements in the window buffers.</typeparam>
public interface IDurableTimeWindowBufferCollection<TKey, TValue> where TKey : notnull
{
    /// <summary>
    /// Gets the number of window buffers in the collection.
    /// </summary>
    int Count { get; }

    /// <summary>
    /// Gets a read-only collection containing the keys of the window buffers.
    /// </summary>
    IReadOnlyCollection<TKey> Keys { get; }

    /// <summary>
    /// Ensures that a window buffer associated with the specified key exists and is configured with the given time window.
    /// </summary>
    /// <param name="key">The key of the window buffer to ensure.</param>
    /// <param name="window">The desired time window for the buffer.</param>
    /// <returns>A durable proxy to the window buffer, which will have the specified window after this call.</returns>
    /// <remarks>
    /// <para>
    /// This method provides a convenient way to get a buffer and set its time window in a single, atomic operation.
    /// If a buffer for the given <paramref name="key"/> does not exist, it will be created with the specified <paramref name="window"/>.
    /// If the buffer already exists, its time window will be overwritten with the new value (if different).
    /// </para>
    /// <para><strong>Decreasing the time window on an existing buffer may result in data loss, if items fall outside the new (smaller) window.</strong></para>
    /// </remarks>
    IDurableTimeWindowBuffer<TValue> EnsureBuffer(TKey key, TimeSpan window);

    /// <summary>
    /// Determines whether the collection contains a window buffer with the specified key.
    /// </summary>
    /// <param name="key">The key to locate.</param>
    /// <returns><c>true</c> if the collection contains a buffer with the key; otherwise, <c>false</c>.</returns>
    bool Contains(TKey key);

    /// <summary>
    /// Removes the window buffer associated with the specified key from the collection.
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
[DebuggerTypeProxy(typeof(DurableTimeWindowBufferCollectionDebugView<,>))]
internal sealed class DurableTimeWindowBufferCollection<TKey, TValue> :
    IDurableTimeWindowBufferCollection<TKey, TValue>, IDurableStateMachine
        where TKey : notnull
{
    private const byte VersionByte = 0;

    private IStateMachineLogWriter? _storage;

    private readonly IFieldCodec<TKey> _keyCodec;
    private readonly IFieldCodec<TValue> _valueCodec;
    private readonly TimeProvider _timeProvider;
    private readonly SerializerSessionPool _sessionPool;
    private readonly Dictionary<TKey, TimeWindowBufferProxy> _proxies = [];

    public DurableTimeWindowBufferCollection(
        [ServiceKey] string key, IStateMachineManager manager,
        IFieldCodec<TKey> keyCodec, IFieldCodec<TValue> valueCodec,
        TimeProvider timeProvider, SerializerSessionPool sessionPool)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);

        _keyCodec = keyCodec;
        _valueCodec = valueCodec;
        _timeProvider = timeProvider;
        _sessionPool = sessionPool;

        manager.RegisterStateMachine(key, this);
    }

    public int Count => _proxies.Count;
    public IReadOnlyCollection<TKey> Keys => _proxies.Keys;

    public IDurableTimeWindowBuffer<TValue> EnsureBuffer(TKey key, TimeSpan window)
    {
        TimeWindowBuffer<TValue>.ThrowIfTooShortWindow((long)window.TotalSeconds);

        var proxy = GetOrCreateProxy(key);
        if (proxy.Window != window)
        {
            proxy.SetWindow(window);
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
            throw new NotSupportedException($"This instance of {nameof(DurableTimeWindowBufferCollection<TKey, TValue>)} supports version {(uint)VersionByte} and not version {(uint)version}.");
        }

        var command = (CommandType)reader.ReadVarUInt32();

        switch (command)
        {
            case CommandType.Snapshot: ApplySnapshot(ref reader); break;
            case CommandType.ClearAll: ApplyClear(); break;
            case CommandType.ClearBuffer: _ = ApplyClearBuffer(ReadKey(ref reader)); break;
            case CommandType.RemoveBuffer: _ = ApplyRemove(ReadKey(ref reader)); break;
            case CommandType.SetWindow: _ = ApplySetBufferWindow(ReadKey(ref reader), (long)reader.ReadVarUInt64()); break;
            case CommandType.EnqueueItem: ApplyEnqueueBufferItem(ReadKey(ref reader), ReadValue(ref reader), (long)reader.ReadVarUInt64()); break;
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
            var currentTimestamp = _timeProvider.GetUtcNow().ToUnixTimeSeconds();

            for (var i = 0; i < bufferCount; i++)
            {
                var key = ReadKey(ref reader);
                var windowSeconds = (long)reader.ReadVarUInt64();
                var itemCount = (int)reader.ReadVarUInt32();

                var buffer = GetOrCreateProxy(key).Buffer;
                buffer.SetWindow(windowSeconds, currentTimestamp);

                for (var j = 0; j < itemCount; j++)
                {
                    var value = ReadValue(ref reader);
                    var timestamp = (long)reader.ReadVarUInt64();

                    buffer.Enqueue(value, timestamp);
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

                writer.WriteVarUInt64((ulong)buffer.WindowSeconds);
                writer.WriteVarUInt32((uint)buffer.Count);

                // Then for each buffer we write its items.
                foreach (var (item, timestamp) in buffer.GetEntries())
                {
                    self._valueCodec.WriteField(ref writer, 0, typeof(TValue), item);
                    writer.WriteVarUInt64((ulong)timestamp);
                }
            }

            writer.Commit();
        }, this);
    }

    private bool SetBufferWindow(TKey key, long windowSeconds)
    {
        if (ApplySetBufferWindow(key, windowSeconds))
        {
            GetStorage().AppendEntry(static (state, bufferWriter) =>
            {
                var (self, key, windowSeconds) = state;

                using var session = self._sessionPool.GetSession();
                
                var writer = Writer.Create(bufferWriter, session);

                writer.WriteByte(VersionByte);
                writer.WriteVarUInt32((uint)CommandType.SetWindow);

                self._keyCodec.WriteField(ref writer, 0, typeof(TKey), key);
                writer.WriteVarUInt64((ulong)windowSeconds);

                writer.Commit();
            }, (this, key, windowSeconds));
            return true;
        }
        return false;
    }

    private void EnqueueItem(TKey key, TValue item)
    {
        var timestamp = _timeProvider.GetUtcNow().ToUnixTimeSeconds();

        ApplyEnqueueBufferItem(key, item, timestamp);

        GetStorage().AppendEntry(static (state, bufferWriter) =>
        {
            var (self, key, item, timestamp) = state;

            using var session = self._sessionPool.GetSession();
            
            var writer = Writer.Create(bufferWriter, session);

            writer.WriteByte(VersionByte);
            writer.WriteVarUInt32((uint)CommandType.EnqueueItem);

            self._keyCodec.WriteField(ref writer, 0, typeof(TKey), key);
            self._valueCodec.WriteField(ref writer, 0, typeof(TValue), item);
            writer.WriteVarUInt64((ulong)timestamp);

            writer.Commit();
        }, (this, key, item, timestamp));
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

    private bool ApplyRemove(TKey key) => _proxies.Remove(key);
    private bool ApplySetBufferWindow(TKey key, long windowSeconds) => GetOrCreateProxy(key).Buffer.SetWindow(windowSeconds, _timeProvider.GetUtcNow().ToUnixTimeSeconds());
    private void ApplyEnqueueBufferItem(TKey key, TValue item, long timestamp) => GetOrCreateProxy(key).Buffer.Enqueue(item, timestamp);
    private bool ApplyTryDequeueBufferItem(TKey key, out TValue item) => GetOrCreateProxy(key).Buffer.TryDequeue(out item!);
    private bool ApplyClearBuffer(TKey key) => GetOrCreateProxy(key).Buffer.Clear();
    private void ApplyClear() => _proxies.Clear();

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal TimeWindowBufferProxy GetOrCreateProxy(TKey key)
    {
        if (!_proxies.TryGetValue(key, out var proxy))
        {
            proxy = new TimeWindowBufferProxy(key, this);
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
        SetWindow = 4,
        EnqueueItem = 5,
        DequeueItem = 6
    }

    internal sealed class TimeWindowBufferProxy(TKey key, 
        DurableTimeWindowBufferCollection<TKey, TValue> collection) : IDurableTimeWindowBuffer<TValue>
    {
        public TimeWindowBuffer<TValue> Buffer { get; } = new();

        public int Count => Buffer.Count;
        public bool IsEmpty => Buffer.IsEmpty;
        public TimeSpan Window => TimeSpan.FromSeconds(Buffer.WindowSeconds);

        public bool Contains(TValue item) => Buffer.Contains(item);
        public bool SetWindow(TimeSpan window) => collection.SetBufferWindow(key, (long)window.TotalSeconds);
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
                Clear(); // We durably log this by means of clearing the buffer.
            }

            return count;
        }

        public int CopyTo(TValue[] array, int arrayIndex) => Buffer.CopyTo(array, arrayIndex);
        public int CopyTo(Span<TValue> destination) => Buffer.CopyTo(destination);

        public IEnumerator<TValue> GetEnumerator() => Buffer.GetEnumerator();
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }
}

internal sealed class DurableTimeWindowBufferCollectionDebugView<TKey, TValue>(
    DurableTimeWindowBufferCollection<TKey, TValue> collection)
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