using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using System.Buffers;
using System.Collections;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace Ledjon.DurableStateMachines;

/// <summary>
/// Defines a durable, hierarchical tree structure where each node is unique.
/// </summary>
/// <typeparam name="T">The type of the values stored in the tree's nodes.</typeparam>
public interface IDurableTree<T> : IEnumerable<T>, IReadOnlyCollection<T> where T : notnull
{
    /// <summary>
    /// Gets the value of the root node of the tree.
    /// </summary>
    /// <exception cref="InvalidOperationException">Thrown if the tree is empty.</exception>
    T Root { get; }

    /// <summary>
    /// Gets a value indicating whether the tree is empty.
    /// </summary>
    bool IsEmpty { get; }

    /// <summary>
    /// Determines whether the tree contains a node with the specified value.
    /// </summary>
    /// <param name="value">The value to locate in the tree.</param>
    /// <returns><c>true</c> if a node with the specified value is found; otherwise, <c>false</c>.</returns>
    bool Contains(T value);

    /// <summary>
    /// Attempts to get the parent of the node with the specified value.
    /// </summary>
    /// <param name="value">The value of the node whose parent is to be found.</param>
    /// <param name="parent">When this method returns, contains the value of the parent node, if found; otherwise, the default value of T.</param>
    /// <returns><c>true</c> if the specified node has a parent; otherwise, <c>false</c>.</returns>
    bool TryGetParent(T value, [MaybeNullWhen(false)] out T parent);

    /// <summary>
    /// Gets a collection containing the direct children of the node with the specified value.
    /// </summary>
    /// <param name="value">The value of the parent node.</param>
    /// <returns>A read-only collection of the direct children. Returns an empty collection if the node has no children or does not exist.</returns>
    IReadOnlyCollection<T> GetChildren(T value);

    /// <summary>
    /// Gets an enumerable collection of all descendants (children, grandchildren, and so on) of the specified node.
    /// </summary>
    /// <param name="value">The value of the ancestor node.</param>
    /// <returns>An enumerable collection of all descendants.</returns>
    IEnumerable<T> GetDescendants(T value);

    /// <summary>
    /// Sets the root node of the tree. This can only be done when the tree is empty.
    /// </summary>
    /// <param name="value">The value to set as the root.</param>
    /// <exception cref="InvalidOperationException">Thrown if the tree is not empty.</exception>
    /// <exception cref="ArgumentException">Thrown if a node with the same value already exists.</exception>
    void SetRoot(T value);

    /// <summary>
    /// Adds a new node with the specified value as a child of an existing parent node.
    /// </summary>
    /// <param name="parent">The value of the parent node.</param>
    /// <param name="value">The value of the new node to add.</param>
    /// <exception cref="ArgumentException">Thrown if the parent node does not exist or if a node with the new value already exists.</exception>
    void Add(T parent, T value);

    /// <summary>
    /// Moves an existing node to become a child of a different parent node.
    /// </summary>
    /// <param name="value">The value of the node to move.</param>
    /// <param name="parent">The value of the new parent node.</param>
    /// <returns><c>true</c> if the node was successfully moved; otherwise, <c>false</c>.</returns>
    /// <exception cref="InvalidOperationException">Thrown if the move creates a cycle or if attempting to move the root node.</exception>
    bool Move(T value, T parent);

    /// <summary>
    /// Removes a node and all of its descendants from the tree.
    /// </summary>
    /// <param name="value">The value of the node to remove.</param>
    /// <returns><c>true</c> if the node was a found and removed; otherwise, <c>false</c>.</returns>
    bool Remove(T value);

    /// <summary>
    /// Removes all nodes from the tree, making it empty.
    /// </summary>
    void Clear();
}

/// <summary>
/// Receives decoded durable tree commands from a codec implementation.
/// </summary>
public interface IDurableTreeCommandHandler<T>
{
    /// <summary>Applies a set root command.</summary>
    void ApplySetRoot(T root);

    /// <summary>Applies an add command.</summary>
    void ApplyAdd(T parent, T value);

    /// <summary>Applies a remove command.</summary>
    void ApplyRemove(T value);

    /// <summary>Applies a move command.</summary>
    void ApplyMove(T value, T parent);

    /// <summary>Applies a clear command.</summary>
    void ApplyClear();

    /// <summary>Resets the receiver before applying replacement entries.</summary>
    void Reset();
}

/// <summary>
/// Serializes one durable tree command and applies one decoded command.
/// </summary>
public interface IDurableTreeCommandCodec<T>
{
    /// <summary>Writes a set root command.</summary>
    void WriteSetRoot(T root, JournalStreamWriter writer);

    /// <summary>Writes an add command.</summary>
    void WriteAdd(T parent, T value, JournalStreamWriter writer);

    /// <summary>Writes a remove command.</summary>
    void WriteRemove(T value, JournalStreamWriter writer);

    /// <summary>Writes a move command.</summary>
    void WriteMove(T value, T parent, JournalStreamWriter writer);

    /// <summary>Writes a clear command.</summary>
    void WriteClear(JournalStreamWriter writer);

    /// <summary>Writes a snapshot command.</summary>
    void WriteSnapshot(int count, T root, IEnumerable<(T Child, T Parent)> edges, JournalStreamWriter writer);

    /// <summary>Reads one encoded command and applies it to <paramref name="handler"/>.</summary>
    void Apply(JournalBufferReader input, IDurableTreeCommandHandler<T> handler);
}

internal sealed class DurableTreeCommandBinaryCodec<T>(
    IFieldCodec<T> codec, SerializerSessionPool sessionPool) : IDurableTreeCommandCodec<T>
{
    private const byte VersionByte = 0;

    private enum CommandType : uint
    {
        Clear = 0,
        Snapshot = 1,
        Add = 2,
        Remove = 3,
        Move = 4,
        SetRoot = 5,
    }

    public void WriteSetRoot(T root, JournalStreamWriter writer)
    {
        using var entry = writer.BeginEntry();
        using var session = sessionPool.GetSession();

        var payloadWriter = Writer.Create(entry.Writer, session);

        payloadWriter.WriteByte(VersionByte);
        payloadWriter.WriteVarUInt32((uint)CommandType.SetRoot);

        codec.WriteField(ref payloadWriter, 0, typeof(T), root);

        payloadWriter.Commit();
        entry.Commit();
    }

    public void WriteAdd(T parent, T value, JournalStreamWriter writer)
    {
        using var entry = writer.BeginEntry();
        using var session = sessionPool.GetSession();

        var payloadWriter = Writer.Create(entry.Writer, session);

        payloadWriter.WriteByte(VersionByte);
        payloadWriter.WriteVarUInt32((uint)CommandType.Add);

        codec.WriteField(ref payloadWriter, 0, typeof(T), parent);
        codec.WriteField(ref payloadWriter, 1, typeof(T), value);

        payloadWriter.Commit();
        entry.Commit();
    }

    public void WriteRemove(T value, JournalStreamWriter writer)
    {
        using var entry = writer.BeginEntry();
        using var session = sessionPool.GetSession();

        var payloadWriter = Writer.Create(entry.Writer, session);

        payloadWriter.WriteByte(VersionByte);
        payloadWriter.WriteVarUInt32((uint)CommandType.Remove);

        codec.WriteField(ref payloadWriter, 0, typeof(T), value);

        payloadWriter.Commit();
        entry.Commit();
    }

    public void WriteMove(T value, T parent, JournalStreamWriter writer)
    {
        using var entry = writer.BeginEntry();
        using var session = sessionPool.GetSession();

        var payloadWriter = Writer.Create(entry.Writer, session);

        payloadWriter.WriteByte(VersionByte);
        payloadWriter.WriteVarUInt32((uint)CommandType.Move);

        codec.WriteField(ref payloadWriter, 0, typeof(T), value);
        codec.WriteField(ref payloadWriter, 1, typeof(T), parent);

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

    public void WriteSnapshot(int count, T root, IEnumerable<(T Child, T Parent)> edges, JournalStreamWriter writer)
    {
        using var entry = writer.BeginEntry();
        using var session = sessionPool.GetSession();

        var payloadWriter = Writer.Create(entry.Writer, session);

        payloadWriter.WriteByte(VersionByte);
        payloadWriter.WriteVarUInt32((uint)CommandType.Snapshot);

        payloadWriter.WriteVarUInt32((uint)count);

        if (count > 0)
        {
            // The root is a special node, so we always write it first.
            codec.WriteField(ref payloadWriter, 0, typeof(T), root);

            foreach (var (child, parent) in edges)
            {
                // For each child found, we write the (child, parent) pair.
                // The 'parent' is the 'current' node from the queue.

                codec.WriteField(ref payloadWriter, 0, typeof(T), child);
                codec.WriteField(ref payloadWriter, 1, typeof(T), parent);
            }
        }

        payloadWriter.Commit();
        entry.Commit();
    }

    public void Apply(JournalBufferReader input, IDurableTreeCommandHandler<T> handler)
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
            case CommandType.SetRoot: handler.ApplySetRoot(ReadValue(ref reader)); break;
            case CommandType.Add: handler.ApplyAdd(ReadValue(ref reader), ReadValue(ref reader)); break;
            case CommandType.Remove: handler.ApplyRemove(ReadValue(ref reader)); break;
            case CommandType.Move: handler.ApplyMove(ReadValue(ref reader), ReadValue(ref reader)); break;
            case CommandType.Clear: handler.ApplyClear(); break;
            case CommandType.Snapshot:
                {
                    var count = (int)reader.ReadVarUInt32();

                    handler.Reset();

                    if (count == 0)
                    {
                        break;
                    }

                    // Because of the level-order guarantee (Breadth-First Search),
                    // the very first item in the snapshot is always the root node.

                    var root = ReadValue(ref reader);
                    handler.ApplySetRoot(root);

                    for (var i = 1; i < count; i++)
                    {
                        // Parent can not be null here, because we are 100% certain the parent node was
                        // processed in a previous iteration (or was the root) and already exists in _nodes.

                        var child = ReadValue(ref reader);
                        var parent = ReadValue(ref reader);

                        handler.ApplyAdd(parent, child);
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

[DebuggerDisplay("Count = {Count}, IsEmpty = {IsEmpty}")]
internal sealed class DurableTree<T> :
    IDurableTree<T>,
    IDurableTreeCommandHandler<T>,
    IJournaledState
        where T : notnull
{
    private JournalStreamWriter? _writer;
    private T? _root;
    private readonly Dictionary<T, Node> _nodes = [];
    private readonly IDurableTreeCommandCodec<T> _codec;

    public DurableTree(
        [ServiceKey] string key, IJournaledStateManager manager,
        IOptions<JournaledStateManagerOptions> options, IServiceProvider serviceProvider)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        _codec = Helpers.GetCodec<IDurableTreeCommandCodec<T>>(serviceProvider, options);
        manager.RegisterState(key, this);
    }

    public int Count => _nodes.Count;
    public bool IsEmpty => _root is null;
    public T Root => _root ?? throw new InvalidOperationException("The tree is empty.");

    #region IJournalState

    IJournaledState IJournaledState.DeepCopy() => throw new NotImplementedException();

    void IJournaledState.ReplayEntry(JournalEntry entry, JournalReplayContext context) =>
        context.GetRequiredCommandCodec(entry.FormatKey, _codec).Apply(entry.Reader, this);

    void IJournaledState.AppendEntries(JournalStreamWriter writer)
    {
        // We use a push model, and append entries upon modification.
    }

    void IJournaledState.AppendSnapshot(JournalStreamWriter snapshotWriter)
    {
        if (_nodes.Count == 0)
        {
            _codec.WriteSnapshot(0, default!, [], snapshotWriter);
        }
        else
        {
            _codec.WriteSnapshot(Count, Root, GetEdges(), snapshotWriter);
        }
    }

    void IJournaledState.Reset(JournalStreamWriter writer)
    {
        ApplyClear();
        _writer = writer;
    }

    #endregion

    #region IDurableTreeCommandHandler

    void IDurableTreeCommandHandler<T>.ApplySetRoot(T root) => ApplySetRoot(root);
    void IDurableTreeCommandHandler<T>.ApplyAdd(T parent, T value) => ApplyAdd(parent, value);
    void IDurableTreeCommandHandler<T>.ApplyRemove(T value) => ApplyRemove(value);
    void IDurableTreeCommandHandler<T>.ApplyMove(T value, T parent) => ApplyMove(value, parent);
    void IDurableTreeCommandHandler<T>.ApplyClear() => ApplyClear();
    void IDurableTreeCommandHandler<T>.Reset() => ApplyClear();

    #endregion

    public void SetRoot(T value)
    {
        if (!EqualityComparer<T>.Default.Equals(_root, default) || _nodes.Count > 0)
        {
            throw new InvalidOperationException("A root node has already been set.");
        }

        if (_nodes.ContainsKey(value))
        {
            throw new ArgumentException("A node with the same value already exists.", nameof(value));
        }

        _codec.WriteSetRoot(value, GetWriter());
        ApplySetRoot(value);
    }

    public void Add(T parent, T value)
    {
        if (!_nodes.ContainsKey(parent))
        {
            throw new ArgumentException("The parent node does not exist.", nameof(parent));
        }

        if (_nodes.ContainsKey(value))
        {
            throw new ArgumentException("A node with the same value already exists.", nameof(value));
        }

        _codec.WriteAdd(parent, value, GetWriter());
        ApplyAdd(parent, value);
    }

    public bool Remove(T value)
    {
        if (!_nodes.ContainsKey(value))
        {
            return false;
        }

        _codec.WriteRemove(value, GetWriter());
        ApplyRemove(value);

        return true;
    }

    public bool Move(T value, T parent)
    {
        if (!Contains(value) || !Contains(parent) || EqualityComparer<T>.Default.Equals(value, parent))
        {
            return false;
        }

        if (IsDescendantOf(parent, value))
        {
            throw new InvalidOperationException("Cannot move a node to be a child of one of its own descendants.");
        }

        if (EqualityComparer<T>.Default.Equals(Root, value) && Count > 1)
        {
            throw new InvalidOperationException("Cannot move the root node.");
        }

        if (!_nodes.TryGetValue(value, out var nodeToMoveData) || nodeToMoveData.Parent is null)
        {
            return false;
        }

        var oldParent = nodeToMoveData.Parent;
        if (EqualityComparer<T>.Default.Equals(oldParent, parent))
        {
            return false;
        }

        _codec.WriteMove(value, parent, GetWriter());
        ApplyMove(value, parent);

        return true;
    }

    public void Clear()
    {
        if (_nodes.Count == 0)
        {
            return;
        }

        _codec.WriteClear(GetWriter());
        ApplyClear();
    }

    public bool Contains(T value) => _nodes.ContainsKey(value);

    public bool TryGetParent(T value, [MaybeNullWhen(false)] out T parent)
    {
        if (_nodes.TryGetValue(value, out var node) && node.Parent is not null)
        {
            parent = node.Parent;
            return true;
        }

        parent = default;
        return false;
    }

    public IReadOnlyCollection<T> GetChildren(T value)
    {
        if (_nodes.TryGetValue(value, out var node))
        {
            return node.Children;
        }

        return [];
    }

    // We return a lazy evaluated enumerable here, because we have to traverse the tree to find the descendants.
    public IEnumerable<T> GetDescendants(T value)
    {
        if (!_nodes.TryGetValue(value, out Node? node))
        {
            yield break;
        }

        using var queue = new ValueQueue();

        foreach (var child in node.Children)
        {
            queue.Enqueue(child);
        }

        while (queue.TryDequeue(out var current))
        {
            yield return current;

            foreach (var grandChild in _nodes[current].Children)
            {
                queue.Enqueue(grandChild);
            }
        }
    }

    private void ApplySetRoot(T root)
    {
        _root = root;
        _nodes[root] = new Node(default);
    }

    private void ApplyAdd(T parentValue, T newValue)
    {
        _nodes[parentValue].Children.Add(newValue);
        _nodes[newValue] = new Node(parentValue);
    }

    private void ApplyRemove(T value)
    {
        if (!_nodes.TryGetValue(value, out var nodeToRemove))
        {
            return;
        }

        // Remove node from its parent's children list.
        if (nodeToRemove.Parent is T parent)
        {
            _nodes[parent].Children.Remove(value);
        }

        // Collect all descendants and remove them.
        using var queue = new ValueQueue();

        queue.Enqueue(value);

        while (queue.TryDequeue(out var current))
        {
            if (_nodes.TryGetValue(current, out var data))
            {
                foreach (var child in data.Children)
                {
                    queue.Enqueue(child);
                }

                _nodes.Remove(current);
            }
        }

        // If the root was removed, clear it.
        if (EqualityComparer<T>.Default.Equals(_root, value))
        {
            _root = default;
        }
    }

    private void ApplyMove(T valueToMove, T newParentValue)
    {
        if (!_nodes.TryGetValue(valueToMove, out var nodeToMoveData) || nodeToMoveData.Parent is null)
        {
            return; // Cannot move root or non just an non-existent node.
        }

        var oldParent = nodeToMoveData.Parent;
        if (EqualityComparer<T>.Default.Equals(oldParent, newParentValue))
        {
            return; // No change, so skip.
        }

        // Unlink from old parent
        _nodes[oldParent].Children.Remove(valueToMove);

        // Link to new parent
        nodeToMoveData.Parent = newParentValue;
        _nodes[newParentValue].Children.Add(valueToMove);
    }

    private void ApplyClear()
    {
        _root = default;
        _nodes.Clear();
    }

    private bool IsDescendantOf(T potentialDescendant, T potentialAncestor)
    {
        using var queue = new ValueQueue();

        queue.Enqueue(potentialAncestor);

        var comparer = EqualityComparer<T>.Default;

        while (queue.TryDequeue(out var current))
        {
            if (comparer.Equals(current, potentialDescendant))
            {
                return true;
            }

            if (_nodes.TryGetValue(current, out var data))
            {
                foreach (var child in data.Children)
                {
                    queue.Enqueue(child);
                }
            }
        }

        return false;
    }

    private IEnumerable<(T Child, T Parent)> GetEdges()
    {
        // We use BFS (Breadth-First Search) to serialize all other nodes, guaranteeing parent-before-child order.
        // We need to serialize the tree into a flat, linear stream of bytes. To rebuild it correctly, a parent node
        // must always be processed before its children. If we process a child first, the ApplySnapshot method will fail
        // when it tries to link that child to a parent that does not exist yet in the reconstructed tree.

        using var queue = new ValueQueue();

        queue.Enqueue(Root);

        while (queue.TryDequeue(out var current))
        {
            var node = _nodes[current];

            foreach (var child in node.Children)
            {
                yield return (child, current);
                queue.Enqueue(child);
            }
        }
    }

    private JournalStreamWriter GetWriter()
    {
        Debug.Assert(_writer.HasValue);
        return _writer.Value;
    }

    public IEnumerator<T> GetEnumerator() => _nodes.Keys.GetEnumerator();
    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    private sealed class Node(T? parent)
    {
        public T? Parent { get; set; } = parent;
        public List<T> Children { get; } = [];
    }

    /// <summary>
    /// A high-performance, minimal-allocation queue implemented as a struct.
    /// It is designed for short-lived, high-frequency operations like the
    /// breadth-first traversals within this class.
    /// </summary>
    /// <remarks>Must be disposed to return the array to the pool.</remarks>
    private struct ValueQueue : IDisposable
    {
        private int _head;
        private int _tail;
        private int _count;
        private T[] _array;

        public ValueQueue()
        {
            _array = ArrayPool<T>.Shared.Rent(4);
        }

        public void Enqueue(T item)
        {
            if (_count == _array.Length)
            {
                Grow();
            }

            _array[_tail] = item;
            _tail = (_tail + 1) % _array.Length;
            _count++;
        }

        public bool TryDequeue([MaybeNullWhen(false)] out T item)
        {
            if (_count == 0)
            {
                item = default;
                return false;
            }

            item = _array[_head];

            _array[_head] = default!; // Clear the slot to allow the GC to collect the object if it is a reference type.
            _head = (_head + 1) % _array.Length;
            _count--;

            return true;
        }

        private void Grow()
        {
            var newArray = ArrayPool<T>.Shared.Rent(_array.Length * 2);

            if (_count > 0)
            {
                // The buffer can be in two states:
                // 1. Contiguous: [_ _, H, I, J, K, T, _, _] where H=head, T=tail.
                // 2. Wrapped:   [J, K, T, _, _, H, I, _,] where the items wrap around the end.
                if (_head < _tail)
                {
                    // The items are in a contiguous block.
                    Array.Copy(_array, _head, newArray, 0, _count);
                }
                else
                {
                
                    int segment = _array.Length - _head;

                    // The items are in two segments. We copy both to the new array.
                    Array.Copy(_array, _head, newArray, 0, segment);
                    Array.Copy(_array, 0, newArray, segment, _tail);
                }
            }

            ArrayPool<T>.Shared.Return(_array, clearArray: true);

            _head = 0;
            _tail = _count; // After copying, the new tail is simply the number of items.
            _array = newArray;
        }

        public void Dispose()
        {
            if (_array != null)
            {
                ArrayPool<T>.Shared.Return(_array, clearArray: true);
                _array = null!; // To make extra sure no reference is kept held.
            }
        }
    }
}

internal sealed class DurableTreeDebugView<T>(IDurableTree<T> tree) where T : notnull
{
    [DebuggerBrowsable(DebuggerBrowsableState.RootHidden)]
    public DebugViewItem[] Items => tree.IsEmpty ? [] : BuildDebugView(tree.Root, tree);

    private static DebugViewItem[] BuildDebugView(T value, IDurableTree<T> tree)
    {
        var children = tree.GetChildren(value).Select(c =>
        {
            var grandChildren = BuildDebugView(c, tree);
            return new DebugViewItem(c, grandChildren);
        }).ToArray();

        return children;
    }

    [DebuggerDisplay("{Value}, Children = {Children.Length}")]
    internal readonly struct DebugViewItem(T value, DebugViewItem[] children)
    {
        [DebuggerBrowsable(DebuggerBrowsableState.Collapsed)]
        public T Value { get; } = value;

        [DebuggerBrowsable(DebuggerBrowsableState.RootHidden)]
        public DebugViewItem[] Children { get; } = children;
    }
}