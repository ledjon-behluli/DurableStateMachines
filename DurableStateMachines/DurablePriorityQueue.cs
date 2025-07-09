using System.Buffers;
using System.Collections;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using Microsoft.Extensions.DependencyInjection;

namespace Ledjon.DurableStateMachines;

/// <summary>
/// Defines a durable, FIFO collection of items that have a value and a priority.
/// On dequeue, the item with the lowest priority value is removed.
/// </summary>
/// <typeparam name="TElement">The type of the elements in the priority queue.</typeparam>
/// <typeparam name="TPriority">The type used to represent the priority of an element.</typeparam>
public interface IDurablePriorityQueue<TElement, TPriority> :
    IEnumerable<(TElement, TPriority)>,
    IReadOnlyCollection<(TElement, TPriority)>
{
    /// <summary>
    /// Removes all items from the priority queue.
    /// </summary>
    void Clear();

    /// <summary>
    /// Returns the element with the lowest priority from the priority queue without removing it.
    /// </summary>
    /// <returns>The element with the lowest priority in the queue.</returns>
    /// <exception cref="InvalidOperationException">Thrown if the queue is empty.</exception>
    TElement Peek();

    /// <summary>
    /// Tries to return the element with the lowest priority from the queue without removing it.
    /// </summary>
    /// <param name="element">When this method returns, contains the object with the lowest priority, if the operation was successful; otherwise, the default value of TElement.</param>
    /// <param name="priority">When this method returns, contains the priority of the element, if the operation was successful; otherwise, the default value of TPriority.</param>
    /// <returns><c>true</c> if there was an element to peek; <c>false</c> if the queue was empty.</returns>
    bool TryPeek([MaybeNullWhen(false)] out TElement element, [MaybeNullWhen(false)] out TPriority priority);

    /// <summary>
    /// Adds the specified element with its associated priority to the priority queue.
    /// </summary>
    /// <param name="element">The element to add to the queue.</param>
    /// <param name="priority">The priority of the element to add.</param>
    void Enqueue(TElement element, TPriority priority);

    /// <summary>
    /// Tries to remove and return the element with the lowest priority from the priority queue.
    /// </summary>
    /// <param name="element">When this method returns, contains the object with the lowest priority that was removed, if the operation was successful; otherwise, the default value of TElement.</param>
    /// <param name="priority">When this method returns, contains the priority of the element that was removed, if the operation was successful; otherwise, the default value of TPriority.</param>
    /// <returns><c>true</c> if an element was successfully removed; <c>false</c> if the queue was empty.</returns>
    bool TryDequeue([MaybeNullWhen(false)] out TElement element, [MaybeNullWhen(false)] out TPriority priority);

    /// <summary>
    /// Removes and returns the element with the lowest priority from the priority queue.
    /// </summary>
    /// <returns>The element that was removed from the queue.</returns>
    /// <exception cref="InvalidOperationException">Thrown if the queue is empty.</exception>
    TElement Dequeue();
}

[DebuggerDisplay("Count = {Count}")]
[DebuggerTypeProxy(typeof(DurablePriorityQueueDebugView<,>))]
internal sealed class DurablePriorityQueue<TElement, TPriority> : IDurablePriorityQueue<TElement, TPriority>, IDurableStateMachine
{
    private const byte VersionByte = 0;

    private IStateMachineLogWriter? _storage;

    private readonly SerializerSessionPool _sessionPool;
    private readonly IFieldCodec<TElement> _elementCodec;
    private readonly IFieldCodec<TPriority> _priorityCodec;
    private readonly PriorityQueue<TElement, TPriority> _items = new();

    public DurablePriorityQueue(
        [ServiceKey] string key, IStateMachineManager manager,
        IFieldCodec<TElement> elementCodec, IFieldCodec<TPriority> priorityCodec,
        SerializerSessionPool sessionPool)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);

        _elementCodec = elementCodec;
        _priorityCodec = priorityCodec;
        _sessionPool = sessionPool;

        manager.RegisterStateMachine(key, this);
    }

    public int Count => _items.Count;

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
            throw new NotSupportedException($"This instance of {nameof(DurablePriorityQueue<TElement, TPriority>)} supports version {(uint)VersionByte} and not version {(uint)version}.");
        }

        var command = (CommandType)reader.ReadVarUInt32();

        switch (command)
        {
            case CommandType.Clear: ApplyClear(); break;
            case CommandType.Snapshot: ApplySnapshot(ref reader); break;
            case CommandType.Enqueue: ApplyEnqueue(ReadElement(ref reader), ReadPriority(ref reader)); break;
            case CommandType.Dequeue: _ = ApplyDequeue(); break;
            default: throw new NotSupportedException($"Command type {command} is not supported");
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        TElement ReadElement(ref Reader<ReadOnlySequenceInput> reader)
        {
            var field = reader.ReadFieldHeader();
            return _elementCodec.ReadValue(ref reader, field);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        TPriority ReadPriority(ref Reader<ReadOnlySequenceInput> reader)
        {
            var field = reader.ReadFieldHeader();
            return _priorityCodec.ReadValue(ref reader, field);
        }

        void ApplySnapshot(ref Reader<ReadOnlySequenceInput> reader)
        {
            var count = (int)reader.ReadVarUInt32();

            ApplyClear();

            _items.EnsureCapacity(count);

            for (var i = 0; i < count; i++)
            {
                ApplyEnqueue(ReadElement(ref reader), ReadPriority(ref reader));
            }
        }
    }

    void IDurableStateMachine.AppendSnapshot(StateMachineStorageWriter writer)
    {
        writer.AppendEntry(static (self, bw) =>
        {
            using var session = self._sessionPool.GetSession();

            var writer = Writer.Create(bw, session);

            writer.WriteByte(VersionByte);
            writer.WriteVarUInt32((uint)CommandType.Snapshot);
            writer.WriteVarUInt32((uint)self._items.Count);

            // We snapshot the complete state by serializing all element-priority pairs.
            // The order of items is irrelevant, as the restore process reconstructs the heap by re-enqueuing each item individually.
            // This correctly restores the logical state of the queue regardless of the snapshot's internal order.

            foreach (var pair in self._items.UnorderedItems)
            {
                self._elementCodec.WriteField(ref writer, 0, typeof(TElement), pair.Element);
                self._priorityCodec.WriteField(ref writer, 1, typeof(TPriority), pair.Priority);
            }

            writer.Commit();
        }, this);
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

    public TElement Peek() => _items.Peek();

    public bool TryPeek(out TElement element, out TPriority priority) => _items.TryPeek(out element!, out priority!);

    public void Enqueue(TElement element, TPriority priority)
    {
        ApplyEnqueue(element, priority);
        GetStorage().AppendEntry(static (state, bufferWriter) =>
        {
            var (self, cmd, element, priority) = state;

            using var session = self._sessionPool.GetSession();

            var writer = Writer.Create(bufferWriter, session);

            writer.WriteByte(VersionByte);
            writer.WriteVarUInt32((uint)cmd);

            self._elementCodec.WriteField(ref writer, 0, typeof(TElement), element);
            self._priorityCodec.WriteField(ref writer, 1, typeof(TPriority), priority);

            writer.Commit();
        }, (this, CommandType.Enqueue, element, priority));
    }

    public TElement Dequeue()
    {
        var element = ApplyDequeue();

        GetStorage().AppendEntry(static (state, bufferWriter) =>
        {
            var (self, cmd) = state;

            using var session = self._sessionPool.GetSession();

            var writer = Writer.Create(bufferWriter, session);

            writer.WriteByte(VersionByte);
            writer.WriteVarUInt32((uint)cmd);

            writer.Commit();
        }, (this, CommandType.Dequeue));

        return element;
    }

    public bool TryDequeue(out TElement element, out TPriority priority)
    {
        if (ApplyTryDequeue(out element, out priority))
        {
            GetStorage().AppendEntry(static (state, bufferWriter) =>
            {
                var (self, cmd) = state;

                using var session = self._sessionPool.GetSession();

                var writer = Writer.Create(bufferWriter, session);

                writer.WriteByte(VersionByte);
                writer.WriteVarUInt32((uint)cmd);

                writer.Commit();
            }, (this, CommandType.Dequeue));

            return true;
        }

        return false;
    }

    private void ApplyClear() => _items.Clear();
    private void ApplyEnqueue(TElement element, TPriority priority) => _items.Enqueue(element, priority);
    private TElement ApplyDequeue() => _items.Dequeue();
    private bool ApplyTryDequeue(out TElement element, out TPriority priority) => _items.TryDequeue(out element!, out priority!);

    private IStateMachineLogWriter GetStorage()
    {
        Debug.Assert(_storage is not null);
        return _storage;
    }

    public IDurableStateMachine DeepCopy() => throw new NotImplementedException();

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    public IEnumerator<(TElement, TPriority)> GetEnumerator() => _items.UnorderedItems.GetEnumerator();

    private enum CommandType : uint
    {
        Clear = 0,
        Snapshot = 1,
        Enqueue = 2,
        Dequeue = 3,
    }
}

internal sealed class DurablePriorityQueueDebugView<TElement, TPriority>(DurablePriorityQueue<TElement, TPriority> queue)
{
    [DebuggerBrowsable(DebuggerBrowsableState.RootHidden)]
    public (TElement Element, TPriority Priority)[] Items
    {
        get
        {
            int i = 0;
            var result = new (TElement, TPriority)[queue.Count];

            foreach (var pair in queue)
            {
                result[i++] = pair;
            }

            return result;
        }
    }
}
