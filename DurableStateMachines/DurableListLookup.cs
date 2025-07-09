using Microsoft.Extensions.DependencyInjection;
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

[DebuggerDisplay("Count = {Count}")]
[DebuggerTypeProxy(typeof(DurableListLookupDebugView<,>))]
internal sealed partial class DurableListLookup<TKey, TValue> :
    IDurableListLookup<TKey, TValue>, IDurableStateMachine
    where TKey : notnull
{
    private const byte VersionByte = 0;

    private IStateMachineLogWriter? _storage;

    private readonly SerializerSessionPool _sessionPool;
    private readonly IFieldCodec<TKey> _keyCodec;
    private readonly IFieldCodec<TValue> _valueCodec;
    private readonly Dictionary<TKey, ValueList> _items = [];

    public DurableListLookup(
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
            throw new NotSupportedException($"This instance of {nameof(DurableListLookup<TKey, TValue>)} supports version {(uint)VersionByte} and not version {(uint)version}.");
        }

        var command = (CommandType)reader.ReadVarUInt32();

        switch (command)
        {
            case CommandType.Add: ApplyAdd(ReadKey(ref reader), ReadValue(ref reader)); break;
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
                    ApplyAdd(key, ReadValue(ref reader));
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
            
            foreach (var (key, list) in self._items)
            {
                self._keyCodec.WriteField(ref writer, 0, typeof(TKey), key);
                writer.WriteVarUInt32((uint)list.Count);
            
                foreach (var value in list)
                {
                    self._valueCodec.WriteField(ref writer, 1, typeof(TValue), value);
                }
            }

            writer.Commit();
        }, this);
    }

    public bool Contains(TKey key) => _items.ContainsKey(key);

    public void Add(TKey key, TValue value)
    {
        ApplyAdd(key, value);
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
               
                using var sess = self._sessionPool.GetSession();
                
                var writer = Writer.Create(bufferWriter, sess);
                
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

    private void ApplyAdd(TKey key, TValue value)
    {
        if (!_items.TryGetValue(key, out var list))
        {
            list = new ValueList(null);
        }

        _items[key] = list.Add(value);
    }

    private bool ApplyRemoveItem(TKey key, TValue value)
    {
        if (!_items.TryGetValue(key, out ValueList list))
        {
            return false;
        }

        var updated = list.Remove(value);
        if (updated.Equals(list))
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
    /// Initializes a new <see cref="ValueList"/> holding either a single value or,
    /// once more than one element is added, an <see cref="ImmutableList{TValue}"/>.
    /// We use <c>ImmutableList&lt;T&gt;</c> rather than <c>List&lt;T&gt;</c> so that:
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
    /// falling back to an <c>ImmutableList&lt;T&gt;</c> only when needed.
    /// </item>
    /// </list>
    /// </summary>
    private readonly struct ValueList(object? value) : IEnumerable<TValue>
    {
        /// <summary>
        /// Stores either a single V or an ImmutableList<V>, we avoid allocating a collection
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
            private ImmutableList<TValue>.Enumerator _values;

            object? IEnumerator.Current => Current;
            public TValue Current => _count > 1 ? _values.Current : _value;

            public Enumerator(ValueList valueList)
            {
                if (valueList._value == null)
                {
                    _value = default;
                    _values = default;
                    _count = 0;
                }
                else
                {
                    if (valueList._value is ImmutableList<TValue> list)
                    {
                        _value = default;
                        _values = list.GetEnumerator();
                        _count = list.Count;

                        Debug.Assert(_count > 1);
                    }
                    else
                    {
                        _value = (TValue)valueList._value;
                        _values = default;
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