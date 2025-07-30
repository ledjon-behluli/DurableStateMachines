using Microsoft.Extensions.DependencyInjection;
using System.Buffers;
using System.Collections;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Ledjon.DurableStateMachines;

/// <summary>
/// Defines a durable collection of unique values that maintains insertion order.
/// </summary>
/// <typeparam name="T">The type of elements in the set.</typeparam>
public interface IDurableOrderedSet<T> : IEnumerable<T>, IReadOnlyCollection<T>
{
    /// <summary>
    /// Provides efficient, zero-allocation, read-only access to the items in their original insertion order.
    /// </summary>
    /// <value>A read-only span containing all elements in the set, in order.</value>
    ReadOnlySpan<T> OrderedItems { get; }

    /// <summary>
    /// Determines whether the set contains a specific value.
    /// </summary>
    /// <param name="item">The item to locate in the set.</param>
    /// <returns><c>true</c> if the item is found in the set; otherwise, <c>false</c>.</returns>
    bool Contains(T item);

    /// <summary>
    /// Adds an element to the end of the ordered set.
    /// </summary>
    /// <param name="item">The element to add to the set.</param>
    /// <returns><c>true</c> if the element is added to the set; <c>false</c> if the element is already present.</returns>
    bool Add(T item);

    /// <summary>
    /// Removes the specified element from the ordered set.
    /// </summary>
    /// <param name="item">The element to remove.</param>
    /// <returns><c>true</c> if the element was successfully found and removed; otherwise, <c>false</c>.</returns>
    bool Remove(T item);

    /// <summary>
    /// Removes all elements from the set.
    /// </summary>
    void Clear();
}

/// <summary>
/// A durable state machine that represents an ordered set.
/// It maintains insertion order and guarantees uniqueness.
/// </summary>
[DebuggerDisplay("Count = {Count}")]
internal sealed class DurableOrderedSet<T> : IDurableOrderedSet<T>, IDurableStateMachine
{
    private const byte VersionByte = 0;

    private IStateMachineLogWriter? _storage;

    private readonly SerializerSessionPool _sessionPool;
    private readonly IFieldCodec<T> _valueCodec;

    private readonly HashSet<T> _set = [];
    private readonly List<T> _list = [];

    public DurableOrderedSet(
        [ServiceKey] string key, IStateMachineManager manager,
        IFieldCodec<T> valueCodec, SerializerSessionPool sessionPool)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);

        _valueCodec = valueCodec;
        _sessionPool = sessionPool;

        manager.RegisterStateMachine(key, this);
    }

    public int Count
    {
        get
        {
            Debug.Assert(_set.Count == _list.Count);
            return _set.Count;
        }
    }

    public ReadOnlySpan<T> OrderedItems => CollectionsMarshal.AsSpan(_list);

    public bool Contains(T item) => _set.Contains(item);

    public bool Add(T item)
    {
        if (ApplyAdd(item))
        {
            GetStorage().AppendEntry(static (state, bufferWriter) =>
            {
                var (self, cmd, itemValue) = state;

                using var session = self._sessionPool.GetSession();
                var writer = Writer.Create(bufferWriter, session);

                writer.WriteByte(VersionByte);
                writer.WriteVarUInt32((uint)cmd);

                self._valueCodec.WriteField(ref writer, 0, typeof(T), itemValue);

                writer.Commit();
            }, (this, CommandType.Add, item));

            return true;
        }

        return false;
    }

    public bool Remove(T item)
    {
        if (ApplyRemove(item))
        {
            GetStorage().AppendEntry(static (state, bufferWriter) =>
            {
                var (self, cmd, itemValue) = state;

                using var session = self._sessionPool.GetSession();
                var writer = Writer.Create(bufferWriter, session);

                writer.WriteByte(VersionByte);
                writer.WriteVarUInt32((uint)cmd);

                self._valueCodec.WriteField(ref writer, 0, typeof(T), itemValue);

                writer.Commit();
            }, (this, CommandType.Remove, item));

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
            throw new NotSupportedException($"Unsupported log version: {version}.");
        }

        var command = (CommandType)reader.ReadVarUInt32();
        switch (command)
        {
            case CommandType.Add: ApplyAdd(ReadValue(ref reader)); break;
            case CommandType.Remove: ApplyRemove(ReadValue(ref reader)); break;
            case CommandType.Clear: ApplyClear(); break;
            case CommandType.Snapshot: ApplySnapshot(ref reader); break;
            default: throw new NotSupportedException($"Command type {command} is not supported.");
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        T ReadValue(ref Reader<ReadOnlySequenceInput> reader)
        {
            var field = reader.ReadFieldHeader();
            return _valueCodec.ReadValue(ref reader, field);
        }

        void ApplySnapshot(ref Reader<ReadOnlySequenceInput> reader)
        {
            var count = (int)reader.ReadVarUInt32();

            ApplyClear();

            _set.EnsureCapacity(count);
            _list.Capacity = count;

            for (var i = 0; i < count; i++)
            {
                ApplyAdd(ReadValue(ref reader));
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
            writer.WriteVarUInt32((uint)self._list.Count);

            // We iterate the list, not the set, to preserve order.
            foreach (var item in self._list)
            {
                self._valueCodec.WriteField(ref writer, 0, typeof(T), item);
            }

            writer.Commit();
        }, this);
    }

    void IDurableStateMachine.AppendEntries(StateMachineStorageWriter writer)
    {
        // We use a push model, and appened entries upon modification.
    }

    private bool ApplyAdd(T item)
    {
        if (_set.Add(item))
        {
            _list.Add(item);
            return true;
        }

        return false;
    }

    private bool ApplyRemove(T item)
    {
        if (_set.Remove(item))
        {
            _list.Remove(item);
            return true;
        }

        return false;
    }

    private void ApplyClear()
    {
        _set.Clear();
        _list.Clear();
    }

    private IStateMachineLogWriter GetStorage()
    {
        Debug.Assert(_storage is not null);
        return _storage;
    }

    // We return the list enumerator since this type is an ordered collection.
    public IEnumerator<T> GetEnumerator() => _list.GetEnumerator();
    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    public IDurableStateMachine DeepCopy() => throw new NotImplementedException();

    private enum CommandType : uint
    {
        Clear = 0,
        Snapshot = 1,
        Add = 2,
        Remove = 3
    }
}