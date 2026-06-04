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
/// Defines a durable, one-to-many dictionary-like collection where each key maps to an ordered list of values.
/// </summary>
/// <typeparam name="TKey">The type of the keys in the lookup.</typeparam>
/// <typeparam name="TValue">The type of the values in the lookup.</typeparam>
/// <remarks>This collection allows duplicate values for a given key.</remarks>
public interface IDurableListLookup<TKey, TValue> :
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
    /// <returns>A read-only collection of values for the specified key, preserving the order of insertion. If the key is not found, an empty collection is returned.</returns>
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
    /// Adds the specified value to the list associated with the specified key.
    /// Duplicate values are allowed.
    /// </summary>
    /// <param name="key">The key of the list to add the value to.</param>
    /// <param name="value">The value to add.</param>
    void Add(TKey key, TValue value);

    /// <summary>
    /// Adds a collection of values to the list associated with the specified key.
    /// </summary>
    /// <param name="key">The key of the list to add the values to.</param>
    /// <param name="values">The collection of values to add.</param>
    void AddRange(TKey key, IEnumerable<TValue> values);

    /// <summary>
    /// Removes the specified key and all its associated values from the lookup.
    /// </summary>
    /// <param name="key">The key to remove.</param>
    /// <returns><c>true</c> if the key was found and removed; otherwise, <c>false</c>.</returns>
    bool Remove(TKey key);

    /// <summary>
    /// Removes the first occurrence of the specified value from the list associated with the specified key.
    /// If the list becomes empty after removal, the key is also removed from the lookup.
    /// </summary>
    /// <param name="key">The key of the list to remove the value from.</param>
    /// <param name="value">The value to remove.</param>
    /// <returns><c>true</c> if the value was found and removed; otherwise, <c>false</c>.</returns>
    bool Remove(TKey key, TValue value);

    /// <summary>
    /// Removes all keys and values from the lookup.
    /// </summary>
    void Clear();
}

/// <summary>
/// Receives decoded durable list lookup commands from a codec implementation.
/// </summary>
public interface IDurableListLookupCommandHandler<TKey, TValue>
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
/// Serializes one durable list lookup command and applies one decoded command.
/// </summary>
public interface IDurableListLookupCommandCodec<TKey, TValue>
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
    void Apply(JournalBufferReader input, IDurableListLookupCommandHandler<TKey, TValue> handler);
}

internal sealed class DurableListLookupCommandBinaryCodec<TKey, TValue>(
    IFieldCodec<TKey> keyCodec, IFieldCodec<TValue> valueCodec, SerializerSessionPool sessionPool)
        : IDurableListLookupCommandCodec<TKey, TValue>
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

        foreach (var (key, list) in items)
        {
            keyCodec.WriteField(ref payloadWriter, 0, typeof(TKey), key);
            payloadWriter.WriteVarUInt32((uint)list.Count);

            foreach (var value in list)
            {
                valueCodec.WriteField(ref payloadWriter, 1, typeof(TValue), value);
            }
        }

        payloadWriter.Commit();
        entry.Commit();
    }

    public void Apply(JournalBufferReader input, IDurableListLookupCommandHandler<TKey, TValue> handler)
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

internal sealed class DurableListLookup<TKey, TValue> :
    IDurableListLookup<TKey, TValue>,
    IDurableListLookupCommandHandler<TKey, TValue>,
    IJournaledState
        where TKey : notnull
{
    private JournalStreamWriter? _writer;
    private readonly Dictionary<TKey, ValueList> _items = [];
    private readonly IDurableListLookupCommandCodec<TKey, TValue> _codec;

    public DurableListLookup(
        [ServiceKey] string key, IJournaledStateManager manager,
        IOptions<JournaledStateManagerOptions> options, IServiceProvider serviceProvider)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        _codec = Helpers.GetCodec<IDurableListLookupCommandCodec<TKey, TValue>>(serviceProvider, options);
        manager.RegisterState(key, this);
    }

    public int Count => _items.Count;
    public IReadOnlyCollection<TKey> Keys => _items.Keys;

    public IReadOnlyCollection<TValue> this[TKey key]
    {
        get
        {
            if (!_items.TryGetValue(key, out var list) || list.Count == 0)
            {
                return ImmutableList<TValue>.Empty;
            }

            if (list.Count == 1)
            {
                return [list.Single()];
            }

            return [.. list];
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

    #region IDurableListLookupCommandHandler

    void IDurableListLookupCommandHandler<TKey, TValue>.ApplyAdd(TKey key, TValue value) => ApplyAdd(key, value);
    void IDurableListLookupCommandHandler<TKey, TValue>.ApplyRemoveKey(TKey key) => ApplyRemoveKey(key);
    void IDurableListLookupCommandHandler<TKey, TValue>.ApplyRemoveItem(TKey key, TValue value) => ApplyRemoveItem(key, value);
    void IDurableListLookupCommandHandler<TKey, TValue>.ApplyClear() => ApplyClear();
    void IDurableListLookupCommandHandler<TKey, TValue>.Reset(int capacityHint)
    {
        ApplyClear();
        _items.EnsureCapacity(capacityHint);
    }

    #endregion

    public bool Contains(TKey key) => _items.ContainsKey(key);

    public bool Contains(TKey key, TValue value) => _items.TryGetValue(key, out var list) && list.Contains(value);

    public void Add(TKey key, TValue value)
    {
        _codec.WriteAdd(key, value, GetWriter());
        ApplyAdd(key, value);
    }

    public void AddRange(TKey key, IEnumerable<TValue> values)
    {
        foreach (var value in values)
        {
            Add(key, value);
        }
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
        if (!_items.TryGetValue(key, out var list))
        {
            list = new ValueList(null);
        }

        _items[key] = list.Add(value);
    }

    private void ApplyRemoveItem(TKey key, TValue value)
    {
        if (!_items.TryGetValue(key, out ValueList list))
        {
            return;
        }

        var updated = list.Remove(value);

        if (updated.Equals(list))
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
    /// Initializes a new <see cref="ValueList"/> holding either a single value or,
    /// once more than one element is added, an <see cref="ImmutableList{TValue}"/>.
    /// We use <see cref="ImmutableList{TValue}"/> rather than <see cref="List{TValue}"/> so that:
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
    /// We avoid unnecessary allocations for the very common “single‐value” case,
    /// falling back to an <see cref="ImmutableList{TValue}"/> only when needed.
    /// </item>
    /// </list>
    /// </summary>
    private readonly struct ValueList(object? value) : IEnumerable<TValue>
    {
        /// <summary>
        /// Stores either a single value or an <see cref="ImmutableList{TValue}"/>, we avoid allocating a collection
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

                if (_value is not ImmutableList<TValue> list)
                {
                    return 1;
                }

                return list.Count;
            }
        }

        public bool Contains(TValue value)
        {
            if (_value is null)
            {
                return false;
            }

            if (_value is ImmutableList<TValue> list)
            {
                return list.Contains(value);
            }

            // Otherwise, we are storing a single item.
            return EqualityComparer<TValue>.Default.Equals((TValue)_value, value);
        }

        public ValueList Add(TValue value)
        {
            if (_value is null)
            {
                // No existing value, we store it directly to avoid list allocation.
                return new ValueList(value);
            }

            // If currently a single value, we wrap it into a 1‑element list; otherwise we cast.
            var list = _value as ImmutableList<TValue> ?? [(TValue)_value];

            // We always append the new value (as list allows duplicates)
            return new ValueList(list.Add(value));
        }

        public ValueList Remove(TValue value)
        {
            // If there's no value stored, there's nothing to remove.
            if (_value is null)
            {
                return this;
            }

            // If we are storing a single value, compare it directly.
            if (_value is TValue single)
            {
                // If it matches, we remove it by clearing the stored value. Otherwise, nothing changes.
                return EqualityComparer<TValue>.Default.Equals(single, value) ? new ValueList(null) : this;
            }

            // We are storing a list, so attempt to remove the value from it.
            var list = (ImmutableList<TValue>)_value;
            var newList = list.Remove(value); // Only removes the first match

            // If the list is now empty, clear the stored value entirely.
            if (newList.Count == 0)
            {
                return new ValueList(null);
            }

            // If only one item remains, store it directly instead of keeping a list.
            if (newList.Count == 1)
            {
                return new ValueList(newList[0]);
            }

            // Otherwise, store the updated list.
            return new ValueList(newList);
        }


        public TValue Single()
        {
            Debug.Assert(_value is TValue);
            return (TValue)_value;
        }

        public bool Equals(ValueList other) => _value == other._value;

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
        IEnumerator<TValue> IEnumerable<TValue>.GetEnumerator() => GetEnumerator();

        public Enumerator GetEnumerator() => new(this);

        public struct Enumerator : IEnumerator<TValue>
        {
            private int _count;
            [AllowNull] private readonly TValue _value;
            private ImmutableList<TValue>.Enumerator _enumerator;

            object? IEnumerator.Current => Current;
            public TValue Current => _count > 1 ? _enumerator.Current : _value;

            internal Enumerator(ValueList valueList)
            {
                if (valueList._value == null)
                {
                    _value = default;
                    _enumerator = default;
                    _count = 0;
                }
                else
                {
                    if (valueList._value is ImmutableList<TValue> list)
                    {
                        _value = default;
                        _enumerator = list.GetEnumerator();
                        _count = list.Count;

                        Debug.Assert(_count > 1);
                    }
                    else
                    {
                        _value = (TValue)valueList._value;
                        _enumerator = default;
                        _count = 1;
                    }
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

internal sealed class DurableListLookupDebugView<TKey, TValue>(DurableListLookup<TKey, TValue> lookup) where TKey : notnull
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