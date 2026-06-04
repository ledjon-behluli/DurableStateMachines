using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using System.Buffers;
using System.Collections;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace Ledjon.DurableStateMachines;

/// <summary>
/// Defines a durable, one-to-many dictionary-like collection where each key maps to an ordered, unique set of values.
/// </summary>
/// <typeparam name="TKey">The type of the keys in the lookup.</typeparam>
/// <typeparam name="TValue">The type of the values in the lookup.</typeparam>
/// <remarks>
/// This collection guarantees uniqueness and maintains insertion order for values associated with a given key.
/// </remarks>
public interface IDurableOrderedSetLookup<TKey, TValue> :
    IEnumerable<(TKey, IReadOnlyCollection<TValue>)>,
    IReadOnlyCollection<(TKey, IReadOnlyCollection<TValue>)>
        where TKey : notnull
{
    /// <summary>
    /// Gets a collection containing all unique keys in the lookup.
    /// </summary>
    IReadOnlyCollection<TKey> Keys { get; }

    /// <summary>
    /// Gets a read-only collection of values associated with the specified key, in the order they were added.
    /// </summary>
    /// <param name="key">The key of the values to get.</param>
    /// <returns>A read-only collection of values for the specified key. If the key is not found, an empty collection is returned.</returns>
    IReadOnlyCollection<TValue> this[TKey key] { get; }

    /// <summary>
    /// Determines whether the lookup contains the specified key.
    /// </summary>
    /// <param name="key">The key to locate in the lookup.</param>
    /// <returns><c>true</c> if the lookup contains an entry for the specified key; otherwise, <c>false</c>.</returns>
    bool Contains(TKey key);

    /// <summary>
    /// Determines if a specific value exists for the given key.
    /// </summary>
    /// <param name="key">The key to look under.</param>
    /// <param name="value">The value to locate.</param>
    /// <returns><c>true</c> if the value is found for the given key; otherwise, <c>false</c>.</returns>
    bool Contains(TKey key, TValue value);

    /// <summary>
    /// Adds the specified value to the ordered set associated with the specified key.
    /// </summary>
    /// <param name="key">The key of the set to add the value to.</param>
    /// <param name="value">The value to add.</param>
    /// <returns><c>true</c> if the value was added to the set for the specified key; <c>false</c> if the value was already present.</returns>
    bool Add(TKey key, TValue value);

    /// <summary>
    /// Removes the specified key and all its associated values from the lookup.
    /// </summary>
    /// <param name="key">The key to remove.</param>
    /// <returns><c>true</c> if the key was found and removed; otherwise, <c>false</c>.</returns>
    bool Remove(TKey key);

    /// <summary>
    /// Removes the specified value from the ordered set associated with the specified key.
    /// If the set becomes empty after removal, the key is also removed from the lookup.
    /// </summary>
    /// <param name="key">The key of the set to remove the value from.</param>
    /// <param name="value">The value to remove.</param>
    /// <returns><c>true</c> if the value was found and removed; otherwise, <c>false</c>.</returns>
    bool Remove(TKey key, TValue value);

    /// <summary>
    /// Removes all keys and values from the lookup.
    /// </summary>
    void Clear();
}

/// <summary>
/// Receives decoded durable ordered set lookup commands from a codec implementation.
/// </summary>
public interface IDurableOrderedSetLookupCommandHandler<TKey, TValue>
{
    /// <summary>Applies an add command.</summary>
    void ApplyAdd(TKey key, TValue value);

    /// <summary>Applies a remove key command.</summary>
    void ApplyRemoveKey(TKey key);

    /// <summary>Applies a remove item command.</summary>
    void ApplyRemoveItem(TKey key, TValue value);

    /// <summary>Applies a clear command.</summary>
    void ApplyClear();

    /// <summary>Resets the receiver before applying replacement entries.</summary>
    void Reset(int capacityHint);
}

/// <summary>
/// Serializes one durable ordered set lookup command and applies one decoded command.
/// </summary>
public interface IDurableOrderedSetLookupCommandCodec<TKey, TValue>
{
    /// <summary>Writes an add command.</summary>
    void WriteAdd(TKey key, TValue value, JournalStreamWriter writer);

    /// <summary>Writes a remove key command.</summary>
    void WriteRemoveKey(TKey key, JournalStreamWriter writer);

    /// <summary>Writes a remove item command.</summary>
    void WriteRemoveItem(TKey key, TValue value, JournalStreamWriter writer);

    /// <summary>Writes a clear command.</summary>
    void WriteClear(JournalStreamWriter writer);

    /// <summary>Writes a snapshot command, deriving the item count from <paramref name="items"/>.</summary>
    void WriteSnapshot(IReadOnlyCollection<(TKey, IReadOnlyCollection<TValue>)> items, JournalStreamWriter writer);

    /// <summary>Reads one encoded command and applies it to <paramref name="handler"/>.</summary>
    void Apply(JournalBufferReader input, IDurableOrderedSetLookupCommandHandler<TKey, TValue> handler);
}

internal sealed class DurableOrderedSetLookupCommandBinaryCodec<TKey, TValue>(
    IFieldCodec<TKey> keyCodec, IFieldCodec<TValue> valueCodec, SerializerSessionPool sessionPool)
        : IDurableOrderedSetLookupCommandCodec<TKey, TValue>
{
    private const byte VersionByte = 0;

    private enum CommandType : uint
    {
        Clear = 0,
        Snapshot = 1,
        Add = 2,
        RemoveKey = 3,
        RemoveItem = 4
    }

    public void WriteAdd(TKey key, TValue value, JournalStreamWriter writer)
    {
        using var entry = writer.BeginEntry();
        using var session = sessionPool.GetSession();

        var payloadWriter = Writer.Create(entry.Writer, session);

        payloadWriter.WriteByte(VersionByte);
        payloadWriter.WriteVarUInt32((uint)CommandType.Add);

        keyCodec.WriteField(ref payloadWriter, 0, typeof(TKey), key);
        valueCodec.WriteField(ref payloadWriter, 1, typeof(TValue), value);

        payloadWriter.Commit();
        entry.Commit();
    }

    public void WriteRemoveKey(TKey key, JournalStreamWriter writer)
    {
        using var entry = writer.BeginEntry();
        using var session = sessionPool.GetSession();

        var payloadWriter = Writer.Create(entry.Writer, session);

        payloadWriter.WriteByte(VersionByte);
        payloadWriter.WriteVarUInt32((uint)CommandType.RemoveKey);

        keyCodec.WriteField(ref payloadWriter, 0, typeof(TKey), key);

        payloadWriter.Commit();
        entry.Commit();
    }

    public void WriteRemoveItem(TKey key, TValue value, JournalStreamWriter writer)
    {
        using var entry = writer.BeginEntry();
        using var session = sessionPool.GetSession();

        var payloadWriter = Writer.Create(entry.Writer, session);

        payloadWriter.WriteByte(VersionByte);
        payloadWriter.WriteVarUInt32((uint)CommandType.RemoveItem);

        keyCodec.WriteField(ref payloadWriter, 0, typeof(TKey), key);
        valueCodec.WriteField(ref payloadWriter, 1, typeof(TValue), value);

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

    public void WriteSnapshot(IReadOnlyCollection<(TKey, IReadOnlyCollection<TValue>)> items, JournalStreamWriter writer)
    {
        using var entry = writer.BeginEntry();
        using var session = sessionPool.GetSession();

        var payloadWriter = Writer.Create(entry.Writer, session);

        payloadWriter.WriteByte(VersionByte);
        payloadWriter.WriteVarUInt32((uint)CommandType.Snapshot);
        payloadWriter.WriteVarUInt32((uint)items.Count);

        foreach (var (key, set) in items)
        {
            keyCodec.WriteField(ref payloadWriter, 0, typeof(TKey), key);
            payloadWriter.WriteVarUInt32((uint)set.Count);

            foreach (var value in set)
            {
                valueCodec.WriteField(ref payloadWriter, 1, typeof(TValue), value);
            }
        }

        payloadWriter.Commit();
        entry.Commit();
    }

    public void Apply(JournalBufferReader input, IDurableOrderedSetLookupCommandHandler<TKey, TValue> handler)
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
            case CommandType.Add: handler.ApplyAdd(ReadKey(ref reader), ReadValue(ref reader)); break;
            case CommandType.RemoveKey: handler.ApplyRemoveKey(ReadKey(ref reader)); break;
            case CommandType.RemoveItem: handler.ApplyRemoveItem(ReadKey(ref reader), ReadValue(ref reader)); break;
            case CommandType.Clear: handler.ApplyClear(); break;
            case CommandType.Snapshot:
                {
                    var keyCount = (int)reader.ReadVarUInt32();
                    handler.Reset(keyCount);

                    for (var i = 0; i < keyCount; i++)
                    {
                        var key = ReadKey(ref reader);
                        var valueCount = (int)reader.ReadVarUInt32();

                        for (var j = 0; j < valueCount; j++)
                        {
                            handler.ApplyAdd(key, ReadValue(ref reader));
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

internal sealed class DurableOrderedSetLookup<TKey, TValue> :
    IDurableOrderedSetLookup<TKey, TValue>,
    IDurableOrderedSetLookupCommandHandler<TKey, TValue>,
    IJournaledState
        where TKey : notnull
{
    private JournalStreamWriter? _writer;
    private readonly Dictionary<TKey, OrderedValueSet> _items = [];
    private readonly IDurableOrderedSetLookupCommandCodec<TKey, TValue> _codec;

    public DurableOrderedSetLookup(
        [ServiceKey] string key, IJournaledStateManager manager,
        IOptions<JournaledStateManagerOptions> options, IServiceProvider serviceProvider)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        _codec = Helpers.GetCodec<IDurableOrderedSetLookupCommandCodec<TKey, TValue>>(serviceProvider, options);
        manager.RegisterState(key, this);
    }

    public int Count => _items.Count;
    public IReadOnlyCollection<TKey> Keys => _items.Keys;

    public IReadOnlyCollection<TValue> this[TKey key]
    {
        get
        {
            if (!_items.TryGetValue(key, out var set) || set.Count == 0)
            {
                return ImmutableList<TValue>.Empty;
            }

            if (set.Count == 1)
            {
                return [set.Single()];
            }

            return set.All();
        }
    }

    #region IJournalState

    IJournaledState IJournaledState.DeepCopy() => throw new NotImplementedException();

    void IJournaledState.ReplayEntry(JournalEntry entry, JournalReplayContext context) =>
        context.GetRequiredCommandCodec(entry.FormatKey, _codec).Apply(entry.Reader, this);

    void IJournaledState.AppendEntries(JournalStreamWriter writer)
    {
        // We use a push model, and append entries upon modification.
    }

    void IJournaledState.AppendSnapshot(JournalStreamWriter snapshotWriter) => _codec.WriteSnapshot(this, snapshotWriter);

    void IJournaledState.Reset(JournalStreamWriter writer)
    {
        ApplyClear();
        _writer = writer;
    }

    #endregion

    #region IDurableOrderedSetLookupCommandHandler

    void IDurableOrderedSetLookupCommandHandler<TKey, TValue>.ApplyAdd(TKey key, TValue value) => ApplyAdd(key, value);
    void IDurableOrderedSetLookupCommandHandler<TKey, TValue>.ApplyRemoveKey(TKey key) => ApplyRemoveKey(key);
    void IDurableOrderedSetLookupCommandHandler<TKey, TValue>.ApplyRemoveItem(TKey key, TValue value) => ApplyRemoveItem(key, value);
    void IDurableOrderedSetLookupCommandHandler<TKey, TValue>.ApplyClear() => ApplyClear();
    void IDurableOrderedSetLookupCommandHandler<TKey, TValue>.Reset(int capacityHint)
    {
        ApplyClear();
        _items.EnsureCapacity(capacityHint);
    }

    #endregion

    public bool Contains(TKey key) => _items.ContainsKey(key);

    public bool Contains(TKey key, TValue value) => _items.TryGetValue(key, out var set) && set.Contains(value);

    public bool Add(TKey key, TValue value)
    {
        if (Contains(key, value))
        {
            return false;
        }

        _codec.WriteAdd(key, value, GetWriter());
        ApplyAdd(key, value);

        return true;
    }

    public bool Remove(TKey key)
    {
        if (!Contains(key))
        {
            return false;
        }

        _codec.WriteRemoveKey(key, GetWriter());
        ApplyRemoveKey(key);

        return true;
    }

    public bool Remove(TKey key, TValue value)
    {
        if (!Contains(key, value))
        {
            return false;
        }

        _codec.WriteRemoveItem(key, value, GetWriter());
        ApplyRemoveItem(key, value);

        return true;
    }

    public void Clear()
    {
        _codec.WriteClear(GetWriter());
        ApplyClear();
    }

    private void ApplyAdd(TKey key, TValue value)
    {
        _items.TryGetValue(key, out var set);

        var updated = set.Add(value);
        
        if (!updated.Equals(set))
        {
            _items[key] = updated;
        }
    }

    private void ApplyRemoveItem(TKey key, TValue value)
    {
        if (!_items.TryGetValue(key, out var set))
        {
            return;
        }

        var updated = set.Remove(value);
        if (updated.Equals(set))
        {
            return;
        }

        if (updated.Count == 0)
        {
            _items.Remove(key);
        }
        else
        {
            _items[key] = updated;
        }
    }

    private void ApplyRemoveKey(TKey key) => _items.Remove(key);

    private void ApplyClear() => _items.Clear();

    private JournalStreamWriter GetWriter()
    {
        Debug.Assert(_writer.HasValue);
        return _writer.Value;
    }

    public IEnumerator<(TKey, IReadOnlyCollection<TValue>)> GetEnumerator()
    {
        foreach (var (key, _) in _items)
        {
            yield return (key, this[key]);
        }
    }

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    /// <summary>
    /// Initializes a new <see cref="OrderedValueSet"/> holding either a single value or, once more than one
    /// element is added, a tuple containing an <see cref="ImmutableHashSet{TValue}"/> and an <see cref="ImmutableList{TValue}"/>.
    /// This composite structure provides both uniqueness with efficient lookups and preservation of insertion order.
    /// <list type="bullet">
    /// <item>
    /// Every mutation (Add/Remove) returns a fresh instance with structural sharing,
    /// ensuring efficient O(log n) updates without copying the entire collection on each change.
    /// </item>
    /// <item>
    /// The state machine can safely snapshot and replay history without risking out‑of‑band
    /// mutations or shared mutable state.
    /// </item>
    /// <item>
    /// We avoid unnecessary allocations for the very common single‐value case, falling back to the
    /// collection tuple only when a second unique element is added.
    /// </item>
    /// </list>
    /// </summary>
    private readonly struct OrderedValueSet(object? value) : IEnumerable<TValue>
    {
        /// <summary>
        /// Stores either a single value, or a tuple of (<see cref="ImmutableHashSet{TValue}"/>, <see cref="ImmutableList{TValue}"/>).
        /// We avoid allocating a collection for keys that only have one value associated with them, which is very common.
        /// </summary>
        internal readonly object? _value = value;

        private static readonly (ImmutableHashSet<TValue> Set, ImmutableList<TValue> List) Empty =
            (ImmutableHashSet<TValue>.Empty, ImmutableList<TValue>.Empty);

        public int Count
        {
            get
            {
                if (_value is null)
                {
                    return 0;
                }

                if (_value is not ValueTuple<ImmutableHashSet<TValue>, ImmutableList<TValue>> tuple)
                {
                    return 1;
                }

                Debug.Assert(tuple.Item1.Count == tuple.Item2.Count);
                return tuple.Item1.Count;
            }
        }

        public bool Contains(TValue value)
        {
            if (_value is null)
            {
                return false;
            }

            // If we have multiple items, we use the hash set for efficient lookup.
            if (_value is ValueTuple<ImmutableHashSet<TValue>, ImmutableList<TValue>> tuple)
            {
                return tuple.Item1.Contains(value);
            }

            // Otherwise, we are storing a single item.
            return EqualityComparer<TValue>.Default.Equals((TValue)_value, value);
        }

        public OrderedValueSet Add(TValue value)
        {
            // If the set is empty, we store the new value directly.
            if (_value is null)
            {
                return new OrderedValueSet(value);
            }

            // If we are already storing multiple items.
            if (_value is ValueTuple<ImmutableHashSet<TValue>, ImmutableList<TValue>> tuple)
            {
                // First we check for existence. If the value is already present, nothing changes.
                if (tuple.Item1.Contains(value))
                {
                    return this;
                }

                // Otherwise, we add the new value to both the set and the list.
                return new OrderedValueSet((tuple.Item1.Add(value), tuple.Item2.Add(value)));
            }

            // If we are storing a single item.
            var singleValue = (TValue)_value;
            if (EqualityComparer<TValue>.Default.Equals(singleValue, value))
            {
                // And it's the same as the new value, nothing changes.
                return this;
            }

            // Otherwise, we upgrade from a single value to the collection tuple.

            var newSet = Empty.Set.Add(singleValue).Add(value);
            var newList = Empty.List.Add(singleValue).Add(value);

            return new OrderedValueSet((newSet, newList));
        }

        public OrderedValueSet Remove(TValue value)
        {
            // If the set is empty, there is nothing to remove.
            if (_value is null)
            {
                return this;
            }

            // If we are storing multiple items.
            if (_value is ValueTuple<ImmutableHashSet<TValue>, ImmutableList<TValue>> tuple)
            {
                // And the value isn't in the set, nothing changes.
                if (!tuple.Item1.Contains(value))
                {
                    return this;
                }

                // Otherwise, remove the value from both collections.
                var newSet = tuple.Item1.Remove(value);
                var newList = tuple.Item2.Remove(value);

                // If more than one item remains, we store the updated tuple.
                if (newList.Count > 1)
                {
                    return new OrderedValueSet((newSet, newList));
                }

                // If exactly one item remains, we downgrade to the single-value optimization.
                if (newList.Count == 1)
                {
                    return new OrderedValueSet(newList[0]);
                }

                // If the set is now empty, we return the default instance.
                return default;
            }

            // If we are storing a single item.
            var singleValue = (TValue)_value;
            if (!EqualityComparer<TValue>.Default.Equals(singleValue, value))
            {
                // And it doesn't match the value to remove, nothing changes.
                return this;
            }

            // Otherwise, the item matches, so we return an default.
            return default;
        }

        public ImmutableList<TValue> All()
        {
            Debug.Assert(_value != null);
            return ((ValueTuple<ImmutableHashSet<TValue>, ImmutableList<TValue>>)_value).Item2;
        }

        public TValue Single()
        {
            Debug.Assert(_value is TValue);
            return (TValue)_value;
        }

        public Enumerator GetEnumerator() => new(this);

        IEnumerator<TValue> IEnumerable<TValue>.GetEnumerator() => GetEnumerator();
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        public bool Equals(OrderedValueSet other) => _value == other._value;

        public struct Enumerator : IEnumerator<TValue>
        {
            private int _count;
            [AllowNull] private readonly TValue _value;
            private ImmutableList<TValue>.Enumerator _enumerator; 

            object? IEnumerator.Current => Current;
            public TValue Current => _count > 1 ? _enumerator.Current : _value;

            internal Enumerator(OrderedValueSet valueSet)
            {
                if (valueSet._value is null)
                {
                    _value = default;
                    _enumerator = default;
                    _count = 0;
                }
                else
                {
                    if (valueSet._value is ValueTuple<ImmutableHashSet<TValue>, ImmutableList<TValue>> tuple)
                    {
                        _value = default;
                        _enumerator = tuple.Item2.GetEnumerator(); // We use the list enumerator since this needs to perserve order.

                        Debug.Assert(tuple.Item1.Count == tuple.Item2.Count);

                        _count = tuple.Item2.Count;

                        Debug.Assert(_count > 1);
                    }
                    else
                    {
                        _value = (TValue?)valueSet._value;
                        _enumerator = default;
                        _count = 1;
                    }
                }
            }

            readonly void IDisposable.Dispose() { }
            void IEnumerator.Reset() => throw new NotSupportedException();

            public bool MoveNext()
            {
                switch (_count)
                {
                    case 0: return false;
                    case 1: _count = 0; return true;
                    default:
                        if (_enumerator.MoveNext())
                        {
                            return true;
                        }
                        _count = 0;
                        return false;
                }
            }
        }
    }
}

internal sealed class DurableOrderedSetLookupDebugView<TKey, TValue>(DurableOrderedSetLookup<TKey, TValue> lookup) where TKey : notnull
{
    [DebuggerBrowsable(DebuggerBrowsableState.RootHidden)]
    public DebugViewItem[] Items => lookup.Select(kvp => new DebugViewItem(kvp.Item1, kvp.Item2)).ToArray();

    [DebuggerDisplay("[{Key}] Count = {Values.Length}")]
    internal readonly struct DebugViewItem(TKey key, IReadOnlyCollection<TValue> values)
    {
        [DebuggerBrowsable(DebuggerBrowsableState.Collapsed)]
        public TKey Key { get; } = key;

        [DebuggerBrowsable(DebuggerBrowsableState.Collapsed)]
        public TValue[] Values { get; } = [.. values];
    }
}