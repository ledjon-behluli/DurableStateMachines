using Microsoft.Extensions.DependencyInjection;
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

[DebuggerDisplay("Count = {Count}")]
[DebuggerTypeProxy(typeof(DurableSetLookupDebugView<,>))]
internal sealed partial class DurableSetLookup<TKey, TValue> :
    IDurableSetLookup<TKey, TValue>, IDurableStateMachine 
    where TKey : notnull
{
    private const byte VersionByte = 0;

    private IStateMachineLogWriter? _storage;

    private readonly SerializerSessionPool _sessionPool;
    private readonly IFieldCodec<TKey> _keyCodec;
    private readonly IFieldCodec<TValue> _valueCodec;
    private readonly Dictionary<TKey, ValueSet> _items = [];

    public DurableSetLookup(
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

    void IDurableStateMachine.AppendEntries(StateMachineStorageWriter writer)
    {
        // We use a push model, and appened entries upon modification.
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
            throw new NotSupportedException($"This instance of {nameof(DurableSetLookup<TKey, TValue>)} supports version {(uint)VersionByte} and not version {(uint)version}.");
        }

        var command = (CommandType)reader.ReadVarUInt32();

        switch (command)
        {
            case CommandType.Add: _ = ApplyAdd(ReadKey(ref reader), ReadValue(ref reader)); break;
            case CommandType.RemoveKey: _ = ApplyRemoveKey(ReadKey(ref reader)); break;
            case CommandType.RemoveItem: _ = ApplyRemoveItem(ReadKey(ref reader), ReadValue(ref reader)); break;
            case CommandType.Clear: ApplyClear(); break;
            case CommandType.Snapshot: ApplySnapshot(ref reader); break;
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
            var keyCount = (int)reader.ReadVarUInt32();

            ApplyClear();
            
            _items.EnsureCapacity(keyCount);

            for (var i = 0; i < keyCount; i++)
            {
                var key = ReadKey(ref reader);
                var valueCount = (int)reader.ReadVarUInt32();

                for (var j = 0; j < valueCount; j++)
                {
                    _ = ApplyAdd(key, ReadValue(ref reader));
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
            writer.WriteVarUInt32((uint)self._items.Count);

            foreach (var (key, set) in self._items)
            {
                self._keyCodec.WriteField(ref writer, 0, typeof(TKey), key);
                writer.WriteVarUInt32((uint)set.Count);

                foreach (var value in set)
                {
                    self._valueCodec.WriteField(ref writer, 1, typeof(TValue), value);
                }
            }

            writer.Commit();
        }, this);
    }

    public bool Contains(TKey key) => _items.ContainsKey(key);

    public bool Add(TKey key, TValue value)
    {
        if (ApplyAdd(key, value))
        {
            GetStorage().AppendEntry(static (state, bufferWriter) =>
            {
                var (self, cmd, key, value) = state;

                using var session = self._sessionPool.GetSession();

                var writer = Writer.Create(bufferWriter, session);

                writer.WriteByte(VersionByte);
                writer.WriteVarUInt32((uint)cmd);

                self._keyCodec.WriteField(ref writer, 0, typeof(TKey), key);
                self._valueCodec.WriteField(ref writer, 1, typeof(TValue), value);

                writer.Commit();
            }, (this, CommandType.Add, key, value));

            return true;
        }

        return false;
    }

    public bool Remove(TKey key)
    {
        if (ApplyRemoveKey(key))
        {
            GetStorage().AppendEntry(static (state, bufferWriter) =>
            {
                var (self, cmd, key) = state;

                using var session = self._sessionPool.GetSession();

                var writer = Writer.Create(bufferWriter, session);

                writer.WriteByte(VersionByte);
                writer.WriteVarUInt32((uint)cmd);

                self._keyCodec.WriteField(ref writer, 0, typeof(TKey), key);

                writer.Commit();
            }, (this, CommandType.RemoveKey, key));

            return true;
        }

        return false;
    }

    public bool Remove(TKey key, TValue value)
    {
        if (ApplyRemoveItem(key, value))
        {
            GetStorage().AppendEntry(static (state, bufferWriter) =>
            {
                var (self, cmd, key, value) = state;

                using var session = self._sessionPool.GetSession();

                var writer = Writer.Create(bufferWriter, session);

                writer.WriteByte(VersionByte);
                writer.WriteVarUInt32((uint)cmd);

                self._keyCodec.WriteField(ref writer, 0, typeof(TKey), key);
                self._valueCodec.WriteField(ref writer, 1, typeof(TValue), value);

                writer.Commit();
            }, (this, CommandType.RemoveItem, key, value));

            return true;
        }

        return false;
    }

    public void Clear()
    {
        ApplyClear();
        GetStorage().AppendEntry(static (state, bufferWriter) =>
        {
            var (self, cmd) = state;

            using var session = self._sessionPool.GetSession();

            var writer = Writer.Create(bufferWriter, session);

            writer.WriteByte(VersionByte);
            writer.WriteVarUInt32((uint)cmd);

            writer.Commit();
        }, (this, CommandType.Clear));
    }

    private bool ApplyAdd(TKey key, TValue value)
    {
        ValueSet updated;

        if (_items.TryGetValue(key, out ValueSet set))
        {
            updated = set.Add(value);
            if (updated.Equals(set))
            {
                return false;
            }
        }
        else
        {
            updated = new ValueSet(value);
        }

        _items[key] = updated;

        return true;
    }

    private bool ApplyRemoveItem(TKey key, TValue value)
    {
        if (!_items.TryGetValue(key, out ValueSet set))
        {
            return false;
        }

        var updated = set.Remove(value);
        if (updated.Equals(set))
        {
            return false;
        }

        if (updated.Count == 0)
        {
            _items.Remove(key);
        }
        else
        {
            _items[key] = updated;
        }

        return true;
    }

    private bool ApplyRemoveKey(TKey key) => _items.Remove(key);
    private void ApplyClear() => _items.Clear();

    private IStateMachineLogWriter GetStorage()
    {
        Debug.Assert(_storage is not null);
        return _storage;
    }

    public IDurableStateMachine DeepCopy() => throw new NotImplementedException();

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    public IEnumerator<(TKey, IReadOnlyCollection<TValue>)> GetEnumerator()
    {
        foreach (var (key, _) in _items)
        {
            yield return (key, this[key]);
        }
    }

    private enum CommandType : uint
    {
        Clear = 0,
        Snapshot = 1,
        Add = 2,
        RemoveKey = 3,
        RemoveItem = 4
    }

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

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
        IEnumerator<TValue> IEnumerable<TValue>.GetEnumerator() => GetEnumerator();

        public Enumerator GetEnumerator() => new(this);

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

        public bool Equals(ValueSet other) => _value == other._value;

        public struct Enumerator : IEnumerator<TValue>
        {
            private int _count;
            [AllowNull] private readonly TValue _value;
            private ImmutableHashSet<TValue>.Enumerator _values;

            object? IEnumerator.Current => Current;
            public TValue Current => _count > 1 ? _values.Current : _value;

            public Enumerator(ValueSet valueSet)
            {
                if (valueSet._value == null)
                {
                    _value = default;
                    _values = default;
                    _count = 0;
                }
                else
                {
                    if (valueSet._value is ImmutableHashSet<TValue> set)
                    {
                        _value = default;
                        _values = set.GetEnumerator();
                        _count = set.Count;

                        Debug.Assert(_count > 1);
                    }
                    else
                    {
                        _value = (TValue)valueSet._value;
                        _values = default;
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
                        if (_values.MoveNext())
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