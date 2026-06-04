using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using System.Collections;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
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
    /// Searches the set for a given value and returns the equal value it finds, if any.
    /// </summary>
    /// <param name="equalValue">The value to search for.</param>
    /// <param name="actualValue">The value from the set that the search found, or the default value of <typeparamref name="T"/> when the search yielded no match.</param>
    /// <returns>A value indicating whether the search was successful.</returns>
    /// <remarks>
    /// This can be useful when you want to reuse a previously stored reference instead of
    /// a newly constructed one (so that more sharing of references can occur) or to look up
    /// a value that has more complete data than the value you currently have, although their
    /// comparer functions indicate they are equal.
    /// </remarks>
    bool TryGetValue(T equalValue, [MaybeNullWhen(false)] out T actualValue);

    /// <summary>
    /// Copies the set to an existing array, starting at the specified array index.
    /// The elements copied preserve their original insertion order.
    /// </summary>
    /// <param name="array">The destination array.</param>
    /// <param name="arrayIndex">The zero-based index in <paramref name="array"/> at which copying begins.</param>
    void CopyTo(T[] array, int arrayIndex);

    /// <summary>
    /// Removes all elements from the set.
    /// </summary>
    void Clear();
}

/// <summary>
/// Receives decoded durable ordered set commands from a codec implementation.
/// </summary>
public interface IDurableOrderedSetCommandHandler<T>
{
    /// <summary>Applies an add command.</summary>
    void ApplyAdd(T item);

    /// <summary>Applies a remove command.</summary>
    void ApplyRemove(T item);

    /// <summary>Applies a clear command.</summary>
    void ApplyClear();

    /// <summary>Resets the receiver before applying replacement entries.</summary>
    void Reset(int capacityHint);
}

/// <summary>
/// Serializes one durable ordered set command and applies one decoded command.
/// </summary>
public interface IDurableOrderedSetCommandCodec<T>
{
    /// <summary>Writes an add command.</summary>
    void WriteAdd(T item, JournalStreamWriter writer);

    /// <summary>Writes a remove command.</summary>
    void WriteRemove(T item, JournalStreamWriter writer);

    /// <summary>Writes a clear command.</summary>
    void WriteClear(JournalStreamWriter writer);

    /// <summary>Writes a snapshot command, deriving the item count from <paramref name="items"/>.</summary>
    void WriteSnapshot(IReadOnlyCollection<T> items, JournalStreamWriter writer);

    /// <summary>Reads one encoded command and applies it to <paramref name="handler"/>.</summary>
    void Apply(JournalBufferReader input, IDurableOrderedSetCommandHandler<T> handler);
}

internal sealed class DurableOrderedSetCommandBinaryCodec<T>(
    IFieldCodec<T> codec, SerializerSessionPool sessionPool) : IDurableOrderedSetCommandCodec<T>
{
    private const byte VersionByte = 0;

    private enum CommandType : uint
    {
        Clear = 0,
        Snapshot = 1,
        Add = 2,
        Remove = 3
    }

    public void WriteAdd(T item, JournalStreamWriter writer)
    {
        using var entry = writer.BeginEntry();
        using var session = sessionPool.GetSession();

        var payloadWriter = Writer.Create(entry.Writer, session);

        payloadWriter.WriteByte(VersionByte);
        payloadWriter.WriteVarUInt32((uint)CommandType.Add);

        codec.WriteField(ref payloadWriter, 0, typeof(T), item);

        payloadWriter.Commit();
        entry.Commit();
    }

    public void WriteRemove(T item, JournalStreamWriter writer)
    {
        using var entry = writer.BeginEntry();
        using var session = sessionPool.GetSession();

        var payloadWriter = Writer.Create(entry.Writer, session);

        payloadWriter.WriteByte(VersionByte);
        payloadWriter.WriteVarUInt32((uint)CommandType.Remove);

        codec.WriteField(ref payloadWriter, 0, typeof(T), item);

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

        // We iterate the list to preserve order.
        foreach (var item in items)
        {
            codec.WriteField(ref payloadWriter, 0, typeof(T), item);
        }

        payloadWriter.Commit();
        entry.Commit();
    }

    public void Apply(JournalBufferReader input, IDurableOrderedSetCommandHandler<T> handler)
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
            case CommandType.Add: handler.ApplyAdd(ReadValue(ref reader)); break;
            case CommandType.Remove: handler.ApplyRemove(ReadValue(ref reader)); break;
            case CommandType.Clear: handler.ApplyClear(); break;
            case CommandType.Snapshot:
                {
                    var count = (int)reader.ReadVarUInt32();
                    handler.Reset(count);
                    for (var i = 0; i < count; i++)
                    {
                        handler.ApplyAdd(ReadValue(ref reader));
                    }
                }
                break;
            default: Helpers.ThrowUnsupportedCommand(command); break;
        }

        Helpers.ThrowIfTrailingData(ref reader);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        T ReadValue<TInput>(ref Reader<TInput> reader)
        {
            var field = reader.ReadFieldHeader();
            return codec.ReadValue(ref reader, field);
        }
    }
}

[DebuggerDisplay("Count = {Count}")]
internal sealed class DurableOrderedSet<T> :
    IDurableOrderedSet<T>,
    IDurableOrderedSetCommandHandler<T>,
    IJournaledState
{
    private JournalStreamWriter? _writer;
    private readonly HashSet<T> _set = [];
    private readonly List<T> _list = [];
    private readonly IDurableOrderedSetCommandCodec<T> _codec;

    public DurableOrderedSet(
        [ServiceKey] string key, IJournaledStateManager manager,
        IOptions<JournaledStateManagerOptions> options, IServiceProvider serviceProvider)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        _codec = Helpers.GetCodec<IDurableOrderedSetCommandCodec<T>>(serviceProvider, options);
        manager.RegisterState(key, this);
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

    #region IDurableOrderedSetCommandHandler

    void IDurableOrderedSetCommandHandler<T>.ApplyAdd(T item) => ApplyAdd(item);
    void IDurableOrderedSetCommandHandler<T>.ApplyRemove(T item) => ApplyRemove(item);
    void IDurableOrderedSetCommandHandler<T>.ApplyClear() => ApplyClear();
    void IDurableOrderedSetCommandHandler<T>.Reset(int capacityHint)
    {
        ApplyClear();
        _set.EnsureCapacity(capacityHint);
        _list.Capacity = capacityHint;
    }

    #endregion

    // We use the list's CopyTo in order to preserve the order when the elements are copied to the destination array.
    public void CopyTo(T[] array, int arrayIndex) => _list.CopyTo(array, arrayIndex);
    public bool Contains(T item) => _set.Contains(item);
    public bool TryGetValue(T equalValue, [MaybeNullWhen(false)] out T actualValue) => _set.TryGetValue(equalValue, out actualValue);

    public bool Add(T item)
    {
        if (_set.Contains(item))
        {
            return false;
        }

        _codec.WriteAdd(item, GetWriter());
        ApplyAdd(item);

        return true;
    }

    public bool Remove(T item)
    {
        if (!_set.Contains(item))
        {
            return false;
        }

        _codec.WriteRemove(item, GetWriter());
        ApplyRemove(item);

        return true;
    }

    public void Clear()
    {
        _codec.WriteClear(GetWriter());
        ApplyClear();
    }

    private void ApplyAdd(T item)
    {
        _set.Add(item);
        _list.Add(item);
    }

    private void ApplyRemove(T item)
    {
        _set.Remove(item);
        _list.Remove(item);
    }

    private void ApplyClear()
    {
        _set.Clear();
        _list.Clear();
    }

    private JournalStreamWriter GetWriter()
    {
        Debug.Assert(_writer.HasValue);
        return _writer.Value;
    }

    // We return the list enumerator since this type is an ordered collection.
    public IEnumerator<T> GetEnumerator() => _list.GetEnumerator();
    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}