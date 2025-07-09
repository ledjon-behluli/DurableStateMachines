using System.Buffers;
using System.Collections;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using Microsoft.Extensions.DependencyInjection;

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
    /// Copies the stack to an existing one-dimensional array, starting at the specified array index.
    /// The elements are copied from top to bottom.
    /// </summary>
    /// <param name="array">The destination one-dimensional array.</param>
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

[DebuggerDisplay("Count = {Count}")]
[DebuggerTypeProxy(typeof(DurableStackDebugView<>))]
internal sealed class DurableStack<T> : IDurableStack<T>, IDurableStateMachine
{
    private const byte VersionByte = 0;

    private IStateMachineLogWriter? _storage;

    private readonly SerializerSessionPool _sessionPool;
    private readonly IFieldCodec<T> _codec;
    private readonly Stack<T> _items = new();

    public DurableStack(
        [ServiceKey] string key, IStateMachineManager manager,
        IFieldCodec<T> codec, SerializerSessionPool sessionPool)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);

        _codec = codec;
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
            throw new NotSupportedException($"This instance of {nameof(DurableStack<T>)} supports version {(uint)VersionByte} and not version {(uint)version}.");
        }

        var command = (CommandType)reader.ReadVarUInt32();

        switch (command)
        {
            case CommandType.Clear: ApplyClear(); break;
            case CommandType.Snapshot: ApplySnapshot(ref reader); break;
            case CommandType.Push: ApplyPush(ReadValue(ref reader)); break;
            case CommandType.Pop: _ = ApplyPop(); break;
            default: throw new NotSupportedException($"Command type {command} is not supported");
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        T ReadValue(ref Reader<ReadOnlySequenceInput> reader)
        {
            var field = reader.ReadFieldHeader();
            return _codec.ReadValue(ref reader, field);
        }

        void ApplySnapshot(ref Reader<ReadOnlySequenceInput> reader)
        {
            var count = (int)reader.ReadVarUInt32();

            ApplyClear();

            _items.EnsureCapacity(count);

            for (var i = 0; i < count; i++)
            {
                ApplyPush(ReadValue(ref reader));
            }
        }
    }

    void IDurableStateMachine.AppendSnapshot(StateMachineStorageWriter snapshotWriter)
    {
        snapshotWriter.AppendEntry(static (self, bufferWriter) =>
        {
            using var session = self._sessionPool.GetSession();

            var writer = Writer.Create(bufferWriter, session);

            writer.WriteByte(VersionByte);
            writer.WriteVarUInt32((uint)CommandType.Snapshot);
            writer.WriteVarUInt32((uint)self._items.Count);

            // Stack enumerates from top-to-bottom, but the restore process pushes items back one-by-one.
            // To reconstruct the original LIFO order, we must reverse the enumeration
            // and write the bottom-most item first, ensuring the final restored state is identical.

            foreach (var item in self._items.Reverse())
            {
                self._codec.WriteField(ref writer, 0, typeof(T), item);
            }

            writer.Commit();
        }, this);
    }

    public bool Contains(T item) => _items.Contains(item);
    public void CopyTo(T[] array, int arrayIndex) => _items.CopyTo(array, arrayIndex);
    public T Peek() => _items.Peek();
    public bool TryPeek([MaybeNullWhen(false)] out T item) => _items.TryPeek(out item);

    public void Push(T item)
    {
        ApplyPush(item);
        GetStorage().AppendEntry(static (state, bufferWriter) =>
        {
            var (self, cmd, item) = state;

            using var session = self._sessionPool.GetSession();

            var writer = Writer.Create(bufferWriter, session);

            writer.WriteByte(VersionByte);
            writer.WriteVarUInt32((uint)cmd);

            self._codec.WriteField(ref writer, 0, typeof(T), item);

            writer.Commit();
        }, (this, CommandType.Push, item));
    }

    public T Pop()
    {
        var result = ApplyPop();
        GetStorage().AppendEntry(static (state, bufferWriter) =>
        {
            (var self, var cmd) = state;

            using var session = self._sessionPool.GetSession();

            var writer = Writer.Create(bufferWriter, session);

            writer.WriteByte(VersionByte);
            writer.WriteVarUInt32((uint)cmd);

            writer.Commit();
        }, (this, CommandType.Pop));

        return result;
    }

    public bool TryPop([MaybeNullWhen(false)] out T item)
    {
        if (ApplyTryPop(out item))
        {
            GetStorage().AppendEntry(static (state, bufferWriter) =>
            {
                (var self, var cmd) = state;

                using var session = self._sessionPool.GetSession();

                var writer = Writer.Create(bufferWriter, session);

                writer.WriteByte(VersionByte);
                writer.WriteVarUInt32((uint)cmd);

                writer.Commit();
            }, (this, CommandType.Pop));

            return true;
        }

        return false;
    }

    public void Clear()
    {
        ApplyClear();
        GetStorage().AppendEntry(static (state, bufferWriter) =>
        {
            (var self, var cmd) = state;

            using var session = self._sessionPool.GetSession();

            var writer = Writer.Create(bufferWriter, session);

            writer.WriteByte(VersionByte);
            writer.WriteVarUInt32((uint)cmd);

            writer.Commit();
        }, (this, CommandType.Clear));
    }

    private T ApplyPop() => _items.Pop();
    private void ApplyPush(T item) => _items.Push(item);
    private bool ApplyTryPop([MaybeNullWhen(false)] out T item) => _items.TryPop(out item!);
    private void ApplyClear() => _items.Clear();

    private IStateMachineLogWriter GetStorage()
    {
        Debug.Assert(_storage is not null);
        return _storage;
    }

    public IDurableStateMachine DeepCopy() => throw new NotImplementedException();

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    public IEnumerator<T> GetEnumerator() => _items.GetEnumerator();

    private enum CommandType : uint
    {
        Clear = 0,
        Snapshot = 1,
        Push = 2,
        Pop = 3
    }
}

internal sealed class DurableStackDebugView<T>(DurableStack<T> stack)
{
    [DebuggerBrowsable(DebuggerBrowsableState.RootHidden)]
    public T[] Items => [.. stack];
}