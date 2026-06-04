using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using System.Collections;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace Ledjon.DurableStateMachines;

/// <summary>
/// Defines a durable, LIFO collection of objects.
/// </summary>
/// <typeparam name="T">Specifies the type of elements in the stack.</typeparam>
public interface IDurableStack<T> : IEnumerable<T>, IReadOnlyCollection<T>
{
    /// <summary>
    /// Removes all objects from the stack.
    /// </summary>
    void Clear();

    /// <summary>
    /// Determines whether an element is in the stack.
    /// </summary>
    /// <param name="item">The object to locate in the stack. The value can be null for reference types.</param>
    /// <returns><c>true</c> if <paramref name="item"/> is found in the stack; otherwise, <c>false</c>.</returns>
    bool Contains(T item);

    /// <summary>
    /// Copies the stack to an existing array, starting at the specified array index.
    /// The elements are copied from top to bottom.
    /// </summary>
    /// <param name="array">The destination  array.</param>
    /// <param name="arrayIndex">The zero-based index in <paramref name="array"/> at which copying begins.</param>
    void CopyTo(T[] array, int arrayIndex);

    /// <summary>
    /// Inserts an object at the top of the stack.
    /// </summary>
    /// <param name="item">The object to push onto the stack. The value can be null for reference types.</param>
    void Push(T item);

    /// <summary>
    /// Removes and returns the object at the top of the stack.
    /// </summary>
    /// <returns>The object removed from the top of the stack.</returns>
    /// <exception cref="InvalidOperationException">Thrown if the stack is empty.</exception>
    T Pop();

    /// <summary>
    /// Tries to remove and return the object at the top of the stack.
    /// </summary>
    /// <param name="item">When this method returns, contains the object removed from the top of the stack, if the operation was successful; otherwise, the default value of T.</param>
    /// <returns><c>true</c> if an object was successfully removed and returned; <c>false</c> if the stack was empty.</returns>
    bool TryPop([MaybeNullWhen(false)] out T item);

    /// <summary>
    /// Returns the object at the top of the stack without removing it.
    /// </summary>
    /// <returns>The object at the top of the stack.</returns>
    /// <exception cref="InvalidOperationException">Thrown if the stack is empty.</exception>
    T Peek();

    /// <summary>
    /// Tries to return the object at the top of the stack without removing it.
    /// </summary>
    /// <param name="item">When this method returns, contains the object at the top of the stack, if one exists; otherwise, the default value of T.</param>
    /// <returns><c>true</c> if there was an object at the top of the stack; <c>false</c> if the stack was empty.</returns>
    bool TryPeek([MaybeNullWhen(false)] out T item);
}

/// <summary>
/// Receives decoded durable stack commands from a codec implementation.
/// </summary>
public interface IDurableStackCommandHandler<T>
{
    /// <summary>Applies a push command.</summary>
    void ApplyPush(T item);

    /// <summary>Applies a pop command.</summary>
    void ApplyPop();

    /// <summary>Applies a clear command.</summary>
    void ApplyClear();

    /// <summary>Resets the receiver before applying replacement entries.</summary>
    void Reset(int capacityHint);
}

/// <summary>
/// Serializes one durable stack command and applies one decoded command.
/// </summary>
public interface IDurableStackCommandCodec<T>
{
    /// <summary>Writes a push command.</summary>
    void WritePush(T item, JournalStreamWriter writer);

    /// <summary>Writes a pop command.</summary>
    void WritePop(JournalStreamWriter writer);

    /// <summary>Writes a clear command.</summary>
    void WriteClear(JournalStreamWriter writer);

    /// <summary>Writes a snapshot command, deriving the item count from <paramref name="items"/>.</summary>
    void WriteSnapshot(IReadOnlyCollection<T> items, JournalStreamWriter writer);

    /// <summary>Reads one encoded command and applies it to <paramref name="handler"/>.</summary>
    void Apply(JournalBufferReader input, IDurableStackCommandHandler<T> handler);
}

internal sealed class DurableStackCommandBinaryCodec<T>(
    IFieldCodec<T> codec, SerializerSessionPool sessionPool) : IDurableStackCommandCodec<T>
{
    private const byte VersionByte = 0;

    private enum CommandType : uint
    {
        Clear = 0,
        Snapshot = 1,
        Push = 2,
        Pop = 3
    }

    public void WritePush(T item, JournalStreamWriter writer)
    {
        using var entry = writer.BeginEntry();
        using var session = sessionPool.GetSession();

        var payloadWriter = Writer.Create(entry.Writer, session);

        payloadWriter.WriteByte(VersionByte);
        payloadWriter.WriteVarUInt32((uint)CommandType.Push);
        
        codec.WriteField(ref payloadWriter, 0, typeof(T), item);

        payloadWriter.Commit();     
        entry.Commit();
    }

    public void WritePop(JournalStreamWriter writer)
    {
        using var entry = writer.BeginEntry();
        using var session = sessionPool.GetSession();

        var payloadWriter = Writer.Create(entry.Writer, session);

        payloadWriter.WriteByte(VersionByte);
        payloadWriter.WriteVarUInt32((uint)CommandType.Pop);

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

    public void WriteSnapshot(IReadOnlyCollection<T> items, JournalStreamWriter writer)
    {
        using var entry = writer.BeginEntry();
        using var session = sessionPool.GetSession();

        var payloadWriter = Writer.Create(entry.Writer, session);

        payloadWriter.WriteByte(VersionByte);
        payloadWriter.WriteVarUInt32((uint)CommandType.Snapshot);
        payloadWriter.WriteVarUInt32((uint)items.Count);

        // Stack enumerates from top-to-bottom, but the restore process pushes items back one-by-one.
        // To reconstruct the original LIFO order, we must reverse the enumeration
        // and write the bottom-most item first, ensuring the final restored state is identical.

        foreach (var item in items.Reverse())
        {
            codec.WriteField(ref payloadWriter, 0, typeof(T), item);
        }

        payloadWriter.Commit();
        entry.Commit();
    }

    public void Apply(JournalBufferReader input, IDurableStackCommandHandler<T> handler)
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
            case CommandType.Push: handler.ApplyPush(ReadValue(ref reader)); break;
            case CommandType.Pop: handler.ApplyPop(); break;
            case CommandType.Clear: handler.ApplyClear(); break;
            case CommandType.Snapshot:
                {
                    var count = (int)reader.ReadVarUInt32();

                    handler.Reset(count);

                    for (var i = 0; i < count; i++)
                    {
                        handler.ApplyPush(ReadValue(ref reader));
                    }
                }
                break;
            default: Helpers.ThrowUnsupportedCommand(command); break;
        }

        Helpers.ThrowIfTrailingData(ref reader);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private T ReadValue<TInput>(ref Reader<TInput> reader)
    {
        var field = reader.ReadFieldHeader();
        return codec.ReadValue(ref reader, field);
    }
}

[DebuggerDisplay("Count = {Count}")]
[DebuggerTypeProxy(typeof(DurableStackDebugView<>))]
internal sealed class DurableStack<T> : 
    IDurableStack<T>, 
    IDurableStackCommandHandler<T>, 
    IJournaledState
{
    private JournalStreamWriter? _writer;
    private readonly Stack<T> _items = new();
    private readonly IDurableStackCommandCodec<T> _codec;

    public DurableStack(
        [ServiceKey] string key, IJournaledStateManager manager,
        IOptions<JournaledStateManagerOptions> options, IServiceProvider serviceProvider)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        _codec = Helpers.GetCodec<IDurableStackCommandCodec<T>>(serviceProvider, options);
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

    #region IDurableStackCommandHandler

    void IDurableStackCommandHandler<T>.ApplyPush(T item) => _items.Push(item);
    void IDurableStackCommandHandler<T>.ApplyPop() => _items.Pop();
    void IDurableStackCommandHandler<T>.ApplyClear() => _items.Clear();
    void IDurableStackCommandHandler<T>.Reset(int capacityHint)
    {
        _items.Clear();
        _items.EnsureCapacity(capacityHint);
    }

    #endregion

    public void Push(T item)
    {
        _codec.WritePush(item, GetWriter());
        _items.Push(item);
    }

    public T Pop()
    {
        var result = _items.Peek(); // If the queue is empty, this throws before we touch the journal stream.

        _codec.WritePop(GetWriter());
        _items.Pop();

        return result;
    }

    public bool TryPop([MaybeNullWhen(false)] out T item)
    {
        if (!_items.TryPeek(out item))
        {
            return false;
        }

        _codec.WritePop(GetWriter());
        _items.Pop();

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

    public bool Contains(T item) => _items.Contains(item);
    public void CopyTo(T[] array, int arrayIndex) => _items.CopyTo(array, arrayIndex);
    public T Peek() => _items.Peek();
    public bool TryPeek([MaybeNullWhen(false)] out T item) => _items.TryPeek(out item);

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    public IEnumerator<T> GetEnumerator() => _items.GetEnumerator();

}

internal sealed class DurableStackDebugView<T>(DurableStack<T> stack)
{
    [DebuggerBrowsable(DebuggerBrowsableState.RootHidden)]
    public T[] Items => [.. stack];
}