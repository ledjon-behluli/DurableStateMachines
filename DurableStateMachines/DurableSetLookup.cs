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
/// Defines a durable, one-to-many dictionary-like collection where each key maps to a unique set of values.
/// </summary>
/// <typeparam name="TKey">The type of the keys in the lookup.</typeparam>
/// <typeparam name="TValue">The type of the values in the lookup.</typeparam>
/// <remarks>This collection does not allow duplicate values for a given key.</remarks>
public interface IDurableSetLookup<TKey, TValue> :
    IEnumerable<(TKey, IReadOnlyCollection<TValue>)>,
    IReadOnlyCollection<(TKey, IReadOnlyCollection<TValue>)>
        where TKey : notnull
{
    /// <summary>
    /// Gets a collection containing all unique keys in the lookup.
    /// </summary>
    IReadOnlyCollection<TKey> Keys { get; }

    /// <summary>
    /// Gets a read-only collection of values associated with the specified key.
    /// </summary>
    /// <param name="key">The key of the values to get.</param>
    /// <returns>A read-only collection of values for the specified key. If the key is not found, an empty collection is returned.</returns>
    IReadOnlyCollection<TValue> this[TKey key] { get; }

    /// <summary>
    /// Determines whether the lookup contains the specified key.
    /// </summary>
    /// <param name="key">The key to locate in the lookup.</param>
    /// <returns><c>true</c> if the lookup contains a key that matches the specified key; otherwise, <c>false</c>.</returns>
    bool Contains(TKey key);

    /// <summary>
    /// Determines if a specific value exists for the given key.
    /// </summary>
    /// <param name="key">The key to look under.</param>
    /// <param name="value">The value to locate.</param>
    /// <returns><c>true</c> if the value is found for the given key; otherwise, <c>false</c>.</returns>
    bool Contains(TKey key, TValue value);

    /// <summary>
    /// Adds the specified value to the set associated with the specified key.
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
    /// Removes the specified value from the set associated with the specified key.
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
/// Receives decoded durable set lookup commands from a codec implementation.
/// </summary>
public interface IDurableSetLookupCommandHandler<TKey, TValue>
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
/// Serializes one durable set lookup command and applies one decoded command.
/// </summary>
public interface IDurableSetLookupCommandCodec<TKey, TValue>
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
    void Apply(JournalBufferReader input, IDurableSetLookupCommandHandler<TKey, TValue> handler);
}

internal sealed class DurableSetLookupCommandBinaryCodec<TKey, TValue>(
    IFieldCodec<TKey> keyCodec, IFieldCodec<TValue> valueCodec, SerializerSessionPool sessionPool)
        : IDurableSetLookupCommandCodec<TKey, TValue>
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

    public void Apply(JournalBufferReader input, IDurableSetLookupCommandHandler<TKey, TValue> handler)
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

internal sealed class DurableSetLookup<TKey, TValue> :
    IDurableSetLookup<TKey, TValue>,
    IDurableSetLookupCommandHandler<TKey, TValue>,
    IJournaledState
        where TKey : notnull
{
    private JournalStreamWriter? _writer;
    private readonly Dictionary<TKey, ValueSet> _items = [];
    private readonly IDurableSetLookupCommandCodec<TKey, TValue> _codec;

    public DurableSetLookup(
        [ServiceKey] string key, IJournaledStateManager manager,
        IOptions<JournaledStateManagerOptions> options, IServiceProvider serviceProvider)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        _codec = Helpers.GetCodec<IDurableSetLookupCommandCodec<TKey, TValue>>(serviceProvider, options);
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
                return ImmutableArray<TValue>.Empty;
            }

            if (set.Count == 1)
            {
                return [set.Single()];
            }

            return [.. set];
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

    #region IDurableSetLookupCommandHandler

    void IDurableSetLookupCommandHandler<TKey, TValue>.ApplyAdd(TKey key, TValue value) => ApplyAdd(key, value);
    void IDurableSetLookupCommandHandler<TKey, TValue>.ApplyRemoveKey(TKey key) => ApplyRemoveKey(key);
    void IDurableSetLookupCommandHandler<TKey, TValue>.ApplyRemoveItem(TKey key, TValue value) => ApplyRemoveItem(key, value);
    void IDurableSetLookupCommandHandler<TKey, TValue>.ApplyClear() => ApplyClear();
    void IDurableSetLookupCommandHandler<TKey, TValue>.Reset(int capacityHint)
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
        if (_items.TryGetValue(key, out var set))
        {
            _items[key] = set.Add(value);
        }
        else
        {
            _items[key] = new ValueSet(value);
        }
    }

    private void ApplyRemoveItem(TKey key, TValue value)
    {
        if (!_items.TryGetValue(key, out ValueSet set))
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
    /// Initializes a new <see cref="ValueSet"/> holding either a single value or,
    /// once more than one element is added, an <see cref="ImmutableHashSet{TValue}"/>.
    /// We use <see cref="ImmutableHashSet{TValue}"/> rather than <see cref="HashSet{TValue}"/> so that:
    /// <list type="bullet">
    /// <item>
    /// Every mutation (Add/Insert/Remove) returns a fresh instance with structural sharing,
    /// ensuring efficient O(log n) updates without copying the entire collection on each change.
    /// </item>
    /// <item>
    /// The state machine can safely snapshot and replay history without risking out‑of‑band
    /// mutations or shared mutable state.
    /// </item>
    /// <item>
    /// We avoid unnecessary allocations for the very common single‐value case,
    /// falling back to an <see cref="ImmutableHashSet{TValue}"/> only when needed.
    /// </item>
    /// </list>
    /// </summary>
    private readonly struct ValueSet(object? value) : IEnumerable<TValue>
    {
        /// <summary>
        /// Stores either a single value or an <see cref="ImmutableHashSet{TValue}"/>, we avoid allocating a collection
        /// for keys that only have one value associated with them, which is very common.
        /// </summary>
        private readonly object? _value = value;

        public int Count
        {
            get
            {
                if (_value is null)
                {
                    return 0;
                }

                if (_value is not ImmutableHashSet<TValue> set)
                {
                    return 1;
                }

                return set.Count;
            }
        }

        public bool Contains(TValue value)
        {
            if (_value is null)
            {
                return false;
            }

            // If we have multiple items, we use the hash set for efficient lookup.
            if (_value is ImmutableHashSet<TValue> set)
            {
                return set.Contains(value);
            }

            // Otherwise, we are storing a single item.
            return EqualityComparer<TValue>.Default.Equals((TValue)_value, value);
        }

        public ValueSet Add(TValue value)
        {
            if (_value is null)
            {
                // No existing value, we store it directly to avoid set allocation.
                return new ValueSet(value);
            }

            // Try to view the stored value as a set.
            var set = _value as ImmutableHashSet<TValue>;

            // If it is not a set, we must have exactly one item stored.
            if (set is null)
            {
                // If the new value equals that single stored element, nothing changes.
                if (ImmutableHashSet<TValue>.Empty.KeyComparer.Equals((TValue)_value, value))
                {
                    return this;
                }

                // Otherwise, we create a new set containing both the old and new items.
                set = [(TValue)_value];
            }

            return new ValueSet(set.Add(value));
        }

        public ValueSet Remove(TValue value)
        {
            // If there's no value stored, there's nothing to remove.
            if (_value is null)
            {
                return this;
            }

            // Try to interpret the stored value as a set.
            if (_value is not ImmutableHashSet<TValue> set)
            {
                // We must be storing a single item.
                if (Comparer.Equals((TValue)_value, value))
                {
                    // It matches the item to remove, so we clear the value entirely.
                    return new ValueSet(null);
                }

                // Otherwise, the item doesn't match and there's nothing to remove.
                return this;
            }

            // Remove the value from the set.
            var newSet = set.Remove(value);

            // If the set is now empty, clear the stored value.
            if (newSet.Count == 0)
            {
                return new ValueSet(null);
            }

            // If there's only one item left, simplify by storing the value directly.
            if (newSet.Count == 1)
            {
                return new ValueSet(newSet.First());
            }

            // Otherwise, store the updated set.
            return new ValueSet(newSet);
        }

        public TValue Single()
        {
            Debug.Assert(_value is TValue);
            return (TValue)_value;
        }

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
        IEnumerator<TValue> IEnumerable<TValue>.GetEnumerator() => GetEnumerator();

        public Enumerator GetEnumerator() => new(this);

        public bool Equals(ValueSet other) => _value == other._value;

        public struct Enumerator : IEnumerator<TValue>
        {
            private int _count;
            [AllowNull] private readonly TValue _value;
            private ImmutableHashSet<TValue>.Enumerator _enumerator;

            object? IEnumerator.Current => Current;
            public TValue Current => _count > 1 ? _enumerator.Current : _value;

            internal Enumerator(ValueSet valueSet)
            {
                if (valueSet._value is null)
                {
                    _value = default;
                    _enumerator = default;
                    _count = 0;
                }
                else
                {
                    if (valueSet._value is ImmutableHashSet<TValue> set)
                    {
                        _value = default;
                        _enumerator = set.GetEnumerator();
                        _count = set.Count;

                        Debug.Assert(_count > 1);
                    }
                    else
                    {
                        _value = (TValue?)valueSet._value;
                        _enumerator = default;
                        _count = 1;
                    }

                    Debug.Assert(_count == valueSet.Count);
                }
            }

            readonly void IDisposable.Dispose() { }
            void IEnumerator.Reset() => throw new NotImplementedException();

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

internal sealed class DurableSetLookupDebugView<TKey, TValue>(DurableSetLookup<TKey, TValue> lookup) where TKey : notnull
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