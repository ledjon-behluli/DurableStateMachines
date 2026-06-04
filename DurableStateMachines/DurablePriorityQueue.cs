using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using System.Collections;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

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

/// <summary>
/// Receives decoded durable priority queue commands from a codec implementation.
/// </summary>
public interface IDurablePriorityQueueCommandHandler<TElement, TPriority>
{
    /// <summary>Applies an enqueue command.</summary>
    void ApplyEnqueue(TElement element, TPriority priority);

    /// <summary>Applies a dequeue command.</summary>
    void ApplyDequeue();

    /// <summary>Applies a try dequeue command.</summary>
    void ApplyTryDequeue();

    /// <summary>Applies a clear command.</summary>
    void ApplyClear();

    /// <summary>Resets the receiver before applying replacement entries.</summary>
    void Reset(int capacityHint);
}

/// <summary>
/// Serializes one durable priority queue command and applies one decoded command.
/// </summary>
public interface IDurablePriorityQueueCommandCodec<TElement, TPriority>
{
    /// <summary>Writes an enqueue command.</summary>
    void WriteEnqueue(TElement element, TPriority priority, JournalStreamWriter writer);

    /// <summary>Writes a dequeue command.</summary>
    void WriteDequeue(JournalStreamWriter writer);

    /// <summary>Writes a clear command.</summary>
    void WriteClear(JournalStreamWriter writer);

    /// <summary>Writes a snapshot command, deriving the item count from <paramref name="items"/>.</summary>
    void WriteSnapshot(IReadOnlyCollection<(TElement, TPriority)> items, JournalStreamWriter writer);

    /// <summary>Reads one encoded command and applies it to <paramref name="handler"/>.</summary>
    void Apply(JournalBufferReader input, IDurablePriorityQueueCommandHandler<TElement, TPriority> handler);
}

internal sealed class DurablePriorityQueueCommandCodec<TElement, TPriority>(
    IFieldCodec<TElement> elementCodec, IFieldCodec<TPriority> priorityCodec, SerializerSessionPool sessionPool)
        : IDurablePriorityQueueCommandCodec<TElement, TPriority>
{
    private const byte VersionByte = 0;

    private enum CommandType : uint
    {
        Clear = 0,
        Snapshot = 1,
        Enqueue = 2,
        Dequeue = 3,
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

    public void WriteDequeue(JournalStreamWriter writer)
    {
        using var entry = writer.BeginEntry();
        using var session = sessionPool.GetSession();

        var payloadWriter = Writer.Create(entry.Writer, session);

        payloadWriter.WriteByte(VersionByte);
        payloadWriter.WriteVarUInt32((uint)CommandType.Dequeue);

        payloadWriter.Commit();
        entry.Commit();
    }

    public void WriteEnqueue(TElement element, TPriority priority, JournalStreamWriter writer)
    {
        using var entry = writer.BeginEntry();
        using var session = sessionPool.GetSession();

        var payloadWriter = Writer.Create(entry.Writer, session);

        payloadWriter.WriteByte(VersionByte);
        payloadWriter.WriteVarUInt32((uint)CommandType.Enqueue);

        elementCodec.WriteField(ref payloadWriter, 0, typeof(TElement), element);
        priorityCodec.WriteField(ref payloadWriter, 1, typeof(TPriority), priority);

        payloadWriter.Commit();
        entry.Commit();
    }

    public void WriteSnapshot(IReadOnlyCollection<(TElement, TPriority)> items, JournalStreamWriter writer)
    {
        using var entry = writer.BeginEntry();
        using var session = sessionPool.GetSession();

        var payloadWriter = Writer.Create(entry.Writer, session);

        payloadWriter.WriteByte(VersionByte);
        payloadWriter.WriteVarUInt32((uint)CommandType.Snapshot);
        payloadWriter.WriteVarUInt32((uint)items.Count);

        // We snapshot the complete state by serializing all element-priority pairs.
        // The order of items is irrelevant, as the restore process reconstructs the heap by re-enqueuing each item individually.
        // This correctly restores the logical state of the queue regardless of the snapshot's internal order.
        // Note that the enumerating over 'items' means enumerating _items.UnorderedItems.GetEnumerator() which is what we want.

        foreach (var (element, priority) in items)
        {
            elementCodec.WriteField(ref payloadWriter, 0, typeof(TElement), element);
            priorityCodec.WriteField(ref payloadWriter, 1, typeof(TPriority), priority);
        }

        payloadWriter.Commit();
        entry.Commit();
    }

    public void Apply(JournalBufferReader input, IDurablePriorityQueueCommandHandler<TElement, TPriority> handler)
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
            case CommandType.Clear: handler.ApplyClear(); break;
            case CommandType.Enqueue: handler.ApplyEnqueue(ReadElement(ref reader), ReadPriority(ref reader)); break;
            case CommandType.Dequeue: handler.ApplyDequeue(); break;
            case CommandType.Snapshot:
                {
                    var count = (int)reader.ReadVarUInt32();

                    handler.Reset(count);

                    for (var i = 0; i < count; i++)
                    {
                        handler.ApplyEnqueue(ReadElement(ref reader), ReadPriority(ref reader));
                    }
                }
                break;
            default: Helpers.ThrowUnsupportedCommand(command); break;
        }

        Helpers.ThrowIfTrailingData(ref reader);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        TElement ReadElement<TInput>(ref Reader<TInput> reader)
        {
            var field = reader.ReadFieldHeader();
            return elementCodec.ReadValue(ref reader, field);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        TPriority ReadPriority<TInput>(ref Reader<TInput> reader)
        {
            var field = reader.ReadFieldHeader();
            return priorityCodec.ReadValue(ref reader, field);
        }
    }
}

[DebuggerDisplay("Count = {Count}")]
[DebuggerTypeProxy(typeof(DurablePriorityQueueDebugView<,>))]
internal sealed class DurablePriorityQueue<TElement, TPriority> :
    IDurablePriorityQueue<TElement, TPriority>,
    IDurablePriorityQueueCommandHandler<TElement, TPriority>,
    IJournaledState
{
    private JournalStreamWriter? _writer;
    private readonly PriorityQueue<TElement, TPriority> _items = new();
    private readonly IDurablePriorityQueueCommandCodec<TElement, TPriority> _codec;

    public DurablePriorityQueue(
        [ServiceKey] string key, IJournaledStateManager manager,
        IOptions<JournaledStateManagerOptions> options, IServiceProvider serviceProvider)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        _codec = Helpers.GetCodec<IDurablePriorityQueueCommandCodec<TElement, TPriority>>(serviceProvider, options);
        manager.RegisterState(key, this);
    }

    public int Count => _items.Count;

    #region IJournalState

    IJournaledState IJournaledState.DeepCopy() => throw new NotImplementedException();

    void IJournaledState.ReplayEntry(JournalEntry entry, JournalReplayContext context) =>
       context.GetRequiredCommandCodec(entry.FormatKey, _codec).Apply(entry.Reader, this);

    void IJournaledState.AppendEntries(JournalStreamWriter writer)
    {
        // We use a push model, and appened entries upon modification.
    }

    void IJournaledState.AppendSnapshot(JournalStreamWriter snapshotWriter) => _codec.WriteSnapshot(this, snapshotWriter);

    void IJournaledState.Reset(JournalStreamWriter writer)
    {
        _items.Clear();
        _writer = writer;
    }

    #endregion

    #region IDurablePriorityQueueCommandHandler

    void IDurablePriorityQueueCommandHandler<TElement, TPriority>.ApplyEnqueue(TElement element, TPriority priority) => _items.Enqueue(element, priority);
    void IDurablePriorityQueueCommandHandler<TElement, TPriority>.ApplyDequeue() => _items.Dequeue();
    void IDurablePriorityQueueCommandHandler<TElement, TPriority>.ApplyTryDequeue() => _items.TryDequeue(out _, out _);
    void IDurablePriorityQueueCommandHandler<TElement, TPriority>.ApplyClear() => _items.Clear();
    void IDurablePriorityQueueCommandHandler<TElement, TPriority>.Reset(int capacityHint)
    {
        _items.Clear();
        _items.EnsureCapacity(capacityHint);
    }

    #endregion

    public TElement Peek() => _items.Peek();

    public bool TryPeek(out TElement element, out TPriority priority) => _items.TryPeek(out element!, out priority!);

    public void Enqueue(TElement element, TPriority priority)
    {
        _codec.WriteEnqueue(element, priority, GetWriter());
        _items.Enqueue(element, priority);
    }

    public TElement Dequeue()
    {
        var result = _items.Peek(); // If the queue is empty, this throws before we touch the journal stream.

        _codec.WriteDequeue(GetWriter());
        _items.Dequeue();

        return result;
    }

    public bool TryDequeue([MaybeNullWhen(false)] out TElement element, [MaybeNullWhen(false)] out TPriority priority)
    {
        // We only want to write to the journal if the queue is not empty.
        // We use TryPeek to check without mutating the state yet.

        if (!_items.TryPeek(out element, out priority))
        {
            return false;
        }

        _codec.WriteDequeue(GetWriter());
        _items.TryDequeue(out _, out _);

        return true;
    }

    public void Clear()
    {
        _codec.WriteClear(GetWriter());
        _items.Clear();
    }

    private JournalStreamWriter GetWriter()
    {
        Debug.Assert(_writer.HasValue);
        return _writer.Value;
    }

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    public IEnumerator<(TElement, TPriority)> GetEnumerator() => _items.UnorderedItems.GetEnumerator();
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
