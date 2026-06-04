using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using System.Buffers;
using System.Collections;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace Ledjon.DurableStateMachines;

/// <summary>
/// Defines a durable, directed graph structure, representing a set of nodes connected by edges.
/// </summary>
/// <typeparam name="TNode">The type of the values stored in the graph's nodes.</typeparam>
/// <typeparam name="TEdge">The type of the values stored in the graph's edges.</typeparam>
public interface IDurableGraph<TNode, TEdge> :
    IEnumerable<(TNode Node, IReadOnlyCollection<(TNode Destination, TEdge Edge)> OutgoingEdges)>,
    IReadOnlyCollection<(TNode Node, IReadOnlyCollection<(TNode Destination, TEdge Edge)> OutgoingEdges)>
        where TNode : notnull
{
    /// <summary>
    /// Gets a collection of all nodes in the graph.
    /// </summary>
    IReadOnlyCollection<TNode> Nodes { get; }

    /// <summary>
    /// Adds a node to the graph.
    /// </summary>
    /// <param name="node">The node to add.</param>
    /// <returns><c>true</c> if the node was added; <c>false</c> if the node already exists.</returns>
    bool AddNode(TNode node);

    /// <summary>
    /// Removes a node and all of its incoming and outgoing edges from the graph.
    /// </summary>
    /// <param name="node">The node to remove.</param>
    /// <returns><c>true</c> if the node was found and removed; otherwise, <c>false</c>.</returns>
    bool RemoveNode(TNode node);

    /// <summary>
    /// Determines whether the graph contains the specified node.
    /// </summary>
    /// <param name="node">The node to locate.</param>
    /// <returns><c>true</c> if the node is found; otherwise, <c>false</c>.</returns>
    bool ContainsNode(TNode node);

    /// <summary>
    /// Adds a directed edge between two nodes. Both nodes must already exist in the graph.
    /// </summary>
    /// <param name="source">The source node of the edge.</param>
    /// <param name="destination">The destination node of the edge.</param>
    /// <param name="edge">The value or data associated with the edge.</param>
    /// <returns><c>true</c> if the edge was added; <c>false</c> if an edge between the specified nodes already exists.</returns>
    /// <exception cref="ArgumentException">Thrown if either the source or destination node does not exist.</exception>
    bool AddEdge(TNode source, TNode destination, TEdge edge);

    /// <summary>
    /// Replaces an existing edge or adds a new one if it does not exist. Both nodes must already exist in the graph.
    /// </summary>
    /// <param name="source">The source node of the edge.</param>
    /// <param name="destination">The destination node of the edge.</param>
    /// <param name="edge">The new value or data to associate with the edge.</param>
    /// <exception cref="ArgumentException">Thrown if either the source or destination node does not exist.</exception>
    void UpsertEdge(TNode source, TNode destination, TEdge edge);

    /// <summary>
    /// Removes the directed edge between two nodes.
    /// </summary>
    /// <param name="source">The source node of the edge.</param>
    /// <param name="destination">The destination node of the edge.</param>
    /// <returns><c>true</c> if the edge was found and removed; otherwise, <c>false</c>.</returns>
    bool RemoveEdge(TNode source, TNode destination);

    /// <summary>
    /// Attempts to get the edge connecting a source node to a destination node.
    /// </summary>
    /// <param name="source">The source node.</param>
    /// <param name="destination">The destination node.</param>
    /// <param name="edge">When this method returns, contains the edge information, if found; otherwise, the default value of TEdge.</param>
    /// <returns><c>true</c> if an edge exists from source to destination; otherwise, <c>false</c>.</returns>
    bool TryGetEdge(TNode source, TNode destination, [MaybeNullWhen(false)] out TEdge edge);

    /// <summary>
    /// Gets a collection of all nodes connected to the specified node by an incoming or outgoing edge.
    /// </summary>
    /// <param name="node">The node whose neighbors to find.</param>
    /// <returns>A read-only collection of all neighbor nodes.</returns>
    IReadOnlyCollection<TNode> GetNeighbors(TNode node);

    /// <summary>
    /// Gets a collection of all outgoing edges from the specified node.
    /// </summary>
    /// <param name="node">The source node.</param>
    /// <returns>A read-only collection of tuples, each containing a destination node and the corresponding edge data.</returns>
    IReadOnlyCollection<(TNode Node, TEdge Edge)> GetOutgoing(TNode node);

    /// <summary>
    /// Gets a collection of all incoming edges to the specified node.
    /// </summary>
    /// <param name="node">The destination node.</param>
    /// <returns>A read-only collection of tuples, each containing a source node and the corresponding edge data.</returns>
    IReadOnlyCollection<(TNode Node, TEdge Edge)> GetIncoming(TNode node);

    /// <summary>
    /// Removes all nodes and edges from the graph.
    /// </summary>
    void Clear();
}

/// <summary>
/// Receives decoded durable graph commands from a codec implementation.
/// </summary>
public interface IDurableGraphCommandHandler<TNode, TEdge>
{
    /// <summary>Applies an add node command.</summary>
    void ApplyAddNode(TNode node);

    /// <summary>Applies a remove node command.</summary>
    void ApplyRemoveNode(TNode node);

    /// <summary>Applies an add edge command.</summary>
    void ApplyAddEdge(TNode source, TNode destination, TEdge edge);

    /// <summary>Applies an upsert edge command.</summary>
    void ApplyUpsertEdge(TNode source, TNode destination, TEdge edge);

    /// <summary>Applies a remove edge command.</summary>
    void ApplyRemoveEdge(TNode source, TNode destination);

    /// <summary>Applies a clear command.</summary>
    void ApplyClear();

    /// <summary>Resets the receiver before applying replacement entries.</summary>
    void Reset();
}

/// <summary>
/// Serializes one durable graph command and applies one decoded command.
/// </summary>
public interface IDurableGraphCommandCodec<TNode, TEdge>
{
    /// <summary>Writes an add node command.</summary>
    void WriteAddNode(TNode node, JournalStreamWriter writer);

    /// <summary>Writes a remove node command.</summary>
    void WriteRemoveNode(TNode node, JournalStreamWriter writer);

    /// <summary>Writes an add edge command.</summary>
    void WriteAddEdge(TNode source, TNode destination, TEdge edge, JournalStreamWriter writer);

    /// <summary>Writes an upsert edge command.</summary>
    void WriteUpsertEdge(TNode source, TNode destination, TEdge edge, JournalStreamWriter writer);

    /// <summary>Writes a remove edge command.</summary>
    void WriteRemoveEdge(TNode source, TNode destination, JournalStreamWriter writer);

    /// <summary>Writes a clear command.</summary>
    void WriteClear(JournalStreamWriter writer);

    /// <summary>Writes a snapshot command.</summary>
    void WriteSnapshot(IReadOnlyCollection<TNode> nodes, int edgeCount, IEnumerable<(TNode Source, TNode Destination, TEdge Edge)> edges, JournalStreamWriter writer);

    /// <summary>Reads one encoded command and applies it to <paramref name="handler"/>.</summary>
    void Apply(JournalBufferReader input, IDurableGraphCommandHandler<TNode, TEdge> handler);
}

internal sealed class DurableGraphCommandBinaryCodec<TNode, TEdge>(
    IFieldCodec<TNode> nodeCodec, IFieldCodec<TEdge> edgeCodec, SerializerSessionPool sessionPool)
        : IDurableGraphCommandCodec<TNode, TEdge>
{
    private const byte VersionByte = 0;

    private enum CommandType : uint
    {
        Clear = 0,
        Snapshot = 1,
        AddNode = 2,
        RemoveNode = 3,
        AddEdge = 4,
        UpsertEdge = 5,
        RemoveEdge = 6
    }

    public void WriteAddNode(TNode node, JournalStreamWriter writer)
    {
        using var entry = writer.BeginEntry();
        using var session = sessionPool.GetSession();

        var payloadWriter = Writer.Create(entry.Writer, session);

        payloadWriter.WriteByte(VersionByte);
        payloadWriter.WriteVarUInt32((uint)CommandType.AddNode);

        nodeCodec.WriteField(ref payloadWriter, 0, typeof(TNode), node);

        payloadWriter.Commit();
        entry.Commit();
    }

    public void WriteRemoveNode(TNode node, JournalStreamWriter writer)
    {
        using var entry = writer.BeginEntry();
        using var session = sessionPool.GetSession();

        var payloadWriter = Writer.Create(entry.Writer, session);

        payloadWriter.WriteByte(VersionByte);
        payloadWriter.WriteVarUInt32((uint)CommandType.RemoveNode);

        nodeCodec.WriteField(ref payloadWriter, 0, typeof(TNode), node);

        payloadWriter.Commit();
        entry.Commit();
    }

    public void WriteAddEdge(TNode source, TNode destination, TEdge edge, JournalStreamWriter writer)
    {
        using var entry = writer.BeginEntry();
        using var session = sessionPool.GetSession();

        var payloadWriter = Writer.Create(entry.Writer, session);

        payloadWriter.WriteByte(VersionByte);
        payloadWriter.WriteVarUInt32((uint)CommandType.AddEdge);

        nodeCodec.WriteField(ref payloadWriter, 0, typeof(TNode), source);
        nodeCodec.WriteField(ref payloadWriter, 1, typeof(TNode), destination);
        edgeCodec.WriteField(ref payloadWriter, 2, typeof(TEdge), edge);

        payloadWriter.Commit();
        entry.Commit();
    }

    public void WriteUpsertEdge(TNode source, TNode destination, TEdge edge, JournalStreamWriter writer)
    {
        using var entry = writer.BeginEntry();
        using var session = sessionPool.GetSession();

        var payloadWriter = Writer.Create(entry.Writer, session);

        payloadWriter.WriteByte(VersionByte);
        payloadWriter.WriteVarUInt32((uint)CommandType.UpsertEdge);

        nodeCodec.WriteField(ref payloadWriter, 0, typeof(TNode), source);
        nodeCodec.WriteField(ref payloadWriter, 1, typeof(TNode), destination);
        edgeCodec.WriteField(ref payloadWriter, 2, typeof(TEdge), edge);

        payloadWriter.Commit();
        entry.Commit();
    }

    public void WriteRemoveEdge(TNode source, TNode destination, JournalStreamWriter writer)
    {
        using var entry = writer.BeginEntry();
        using var session = sessionPool.GetSession();

        var payloadWriter = Writer.Create(entry.Writer, session);

        payloadWriter.WriteByte(VersionByte);
        payloadWriter.WriteVarUInt32((uint)CommandType.RemoveEdge);

        nodeCodec.WriteField(ref payloadWriter, 0, typeof(TNode), source);
        nodeCodec.WriteField(ref payloadWriter, 1, typeof(TNode), destination);

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

    public void WriteSnapshot(IReadOnlyCollection<TNode> nodes, int edgeCount, IEnumerable<(TNode Source, TNode Destination, TEdge Edge)> edges, JournalStreamWriter writer)
    {
        using var entry = writer.BeginEntry();
        using var session = sessionPool.GetSession();

        var payloadWriter = Writer.Create(entry.Writer, session);

        payloadWriter.WriteByte(VersionByte);
        payloadWriter.WriteVarUInt32((uint)CommandType.Snapshot);

        // The snapshot format is the list of all nodes, followed by the list of all edges.
        // This strategy avoids complex traversal logic for graphs, which can
        // contain cycles or consist of multiple, disconnected components.

        // First, write the count of all nodes, followed by each node's value.
        payloadWriter.WriteVarUInt32((uint)nodes.Count);

        foreach (var node in nodes)
        {
            nodeCodec.WriteField(ref payloadWriter, 0, typeof(TNode), node);
        }

        // Next, calculate and write the total number of edges.
        payloadWriter.WriteVarUInt32((uint)edgeCount);

        // Finally, iterate through every outgoing edge in the graph and write it as a (source, dest, edge) triplet.
        foreach (var (source, dest, edge) in edges)
        {
            nodeCodec.WriteField(ref payloadWriter, 0, typeof(TNode), source);
            nodeCodec.WriteField(ref payloadWriter, 1, typeof(TNode), dest);
            edgeCodec.WriteField(ref payloadWriter, 2, typeof(TEdge), edge);
        }

        payloadWriter.Commit();
        entry.Commit();
    }

    public void Apply(JournalBufferReader input, IDurableGraphCommandHandler<TNode, TEdge> handler)
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
            case CommandType.AddNode: handler.ApplyAddNode(ReadNode(ref reader)); break;
            case CommandType.RemoveNode: handler.ApplyRemoveNode(ReadNode(ref reader)); break;
            case CommandType.AddEdge: handler.ApplyAddEdge(ReadNode(ref reader), ReadNode(ref reader), ReadEdge(ref reader)); break;
            case CommandType.UpsertEdge: handler.ApplyUpsertEdge(ReadNode(ref reader), ReadNode(ref reader), ReadEdge(ref reader)); break;
            case CommandType.RemoveEdge: handler.ApplyRemoveEdge(ReadNode(ref reader), ReadNode(ref reader)); break;
            case CommandType.Clear: handler.ApplyClear(); break;
            case CommandType.Snapshot:
                {
                    handler.Reset();

                    // First, read the node count and reconstruct all nodes. This ensures that when we
                    // process the edges in the next step, both source and destination nodes will already exist.
                    var nodeCount = (int)reader.ReadVarUInt32();
                    for (var i = 0; i < nodeCount; i++)
                    {
                        handler.ApplyAddNode(ReadNode(ref reader));
                    }

                    // Next, read the edge count and reconstruct all edges,
                    // linking the nodes that were created in the previous step.
                    var edgeCount = (int)reader.ReadVarUInt32();
                    for (var i = 0; i < edgeCount; i++)
                    {
                        var source = ReadNode(ref reader);
                        var destination = ReadNode(ref reader);
                        var edge = ReadEdge(ref reader);

                        handler.ApplyAddEdge(source, destination, edge);
                    }
                }
                break;
            default: Helpers.ThrowUnsupportedCommand(command); break;
        }

        Helpers.ThrowIfTrailingData(ref reader);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        TNode ReadNode<TInput>(ref Reader<TInput> reader)
        {
            var field = reader.ReadFieldHeader();
            return nodeCodec.ReadValue(ref reader, field);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        TEdge ReadEdge<TInput>(ref Reader<TInput> reader)
        {
            var field = reader.ReadFieldHeader();
            return edgeCodec.ReadValue(ref reader, field);
        }
    }
}

[DebuggerDisplay("Nodes = {Count}")]
internal sealed class DurableGraph<TNode, TEdge> :
    IDurableGraph<TNode, TEdge>,
    IDurableGraphCommandHandler<TNode, TEdge>,
    IJournaledState
        where TNode : notnull
{
    private JournalStreamWriter? _writer;
    private readonly Dictionary<TNode, NodeInfo> _nodes = [];
    private readonly IDurableGraphCommandCodec<TNode, TEdge> _codec;

    public DurableGraph(
        [ServiceKey] string key, IJournaledStateManager manager,
        IOptions<JournaledStateManagerOptions> options, IServiceProvider serviceProvider)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        _codec = Helpers.GetCodec<IDurableGraphCommandCodec<TNode, TEdge>>(serviceProvider, options);
        manager.RegisterState(key, this);
    }

    public int Count => _nodes.Count;
    public IReadOnlyCollection<TNode> Nodes => _nodes.Keys;

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
        var edgeCount = _nodes.Values.Sum(n => n.Outgoing.Count);
        _codec.WriteSnapshot(Nodes, edgeCount, GetEdges(), snapshotWriter);
    }

    void IJournaledState.Reset(JournalStreamWriter writer)
    {
        ApplyClear();
        _writer = writer;
    }

    #endregion

    #region IDurableGraphCommandHandler

    void IDurableGraphCommandHandler<TNode, TEdge>.ApplyAddNode(TNode node) => ApplyAddNode(node);
    void IDurableGraphCommandHandler<TNode, TEdge>.ApplyRemoveNode(TNode node) => ApplyRemoveNode(node);
    void IDurableGraphCommandHandler<TNode, TEdge>.ApplyAddEdge(TNode source, TNode destination, TEdge edge) => ApplyAddEdge(source, destination, edge);
    void IDurableGraphCommandHandler<TNode, TEdge>.ApplyUpsertEdge(TNode source, TNode destination, TEdge edge) => ApplyUpsertEdge(source, destination, edge);
    void IDurableGraphCommandHandler<TNode, TEdge>.ApplyRemoveEdge(TNode source, TNode destination) => ApplyRemoveEdge(source, destination);
    void IDurableGraphCommandHandler<TNode, TEdge>.ApplyClear() => ApplyClear();
    void IDurableGraphCommandHandler<TNode, TEdge>.Reset() => ApplyClear();

    #endregion

    public bool AddNode(TNode node)
    {
        if (_nodes.ContainsKey(node))
        {
            return false;
        }

        _codec.WriteAddNode(node, GetWriter());
        ApplyAddNode(node);

        return true;
    }

    public bool RemoveNode(TNode node)
    {
        if (!_nodes.ContainsKey(node))
        {
            return false;
        }

        _codec.WriteRemoveNode(node, GetWriter());
        ApplyRemoveNode(node);

        return true;
    }

    public bool AddEdge(TNode source, TNode destination, TEdge edge)
    {
        if (!_nodes.ContainsKey(source))
        {
            throw new ArgumentException("The source node does not exist.", nameof(source));
        }

        if (!_nodes.ContainsKey(destination))
        {
            throw new ArgumentException("The destination node does not exist.", nameof(destination));
        }

        if (_nodes[source].Outgoing.ContainsKey(destination))
        {
            return false;
        }

        _codec.WriteAddEdge(source, destination, edge, GetWriter());
        ApplyAddEdge(source, destination, edge);

        return true;
    }

    public void UpsertEdge(TNode source, TNode destination, TEdge edge)
    {
        if (!_nodes.ContainsKey(source))
        {
            throw new ArgumentException("The source node does not exist.", nameof(source));
        }

        if (!_nodes.ContainsKey(destination))
        {
            throw new ArgumentException("The destination node does not exist.", nameof(destination));
        }

        // Since validations passed, the upsert will always succeed:
        // - If the edge does not exist, it is created.
        // - If the edge exists with a different value, it is updated.
        // - If the edge exists with the exact same value, it is overwritten with that same value.

        _codec.WriteUpsertEdge(source, destination, edge, GetWriter());
        ApplyUpsertEdge(source, destination, edge);
    }

    public bool RemoveEdge(TNode source, TNode destination)
    {
        if (!_nodes.TryGetValue(source, out var sourceInfo) || !sourceInfo.Outgoing.ContainsKey(destination))
        {
            return false;
        }

        _codec.WriteRemoveEdge(source, destination, GetWriter());
        ApplyRemoveEdge(source, destination);

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

    public bool ContainsNode(TNode node) => _nodes.ContainsKey(node);

    public bool TryGetEdge(TNode source, TNode destination, [MaybeNullWhen(false)] out TEdge edge)
    {
        if (_nodes.TryGetValue(source, out var sourceInfo) &&
            sourceInfo.Outgoing.TryGetValue(destination, out edge))
        {
            return true;
        }

        edge = default;

        return false;
    }

    public IReadOnlyCollection<TNode> GetNeighbors(TNode node)
    {
        if (!_nodes.TryGetValue(node, out var nodeInfo))
        {
            return [];
        }

        var neighbors = new HashSet<TNode>(nodeInfo.Outgoing.Keys);
        neighbors.UnionWith(nodeInfo.Incoming);

        return neighbors;
    }

    public IReadOnlyCollection<(TNode Node, TEdge Edge)> GetOutgoing(TNode node)
    {
        if (!_nodes.TryGetValue(node, out var nodeInfo))
        {
            return [];
        }

        return nodeInfo.Outgoing.Select(kvp => (kvp.Key, kvp.Value)).ToList();
    }

    public IReadOnlyCollection<(TNode Node, TEdge Edge)> GetIncoming(TNode node)
    {
        if (!_nodes.TryGetValue(node, out var nodeInfo))
        {
            return [];
        }

        var result = new List<(TNode Node, TEdge Edge)>();

        foreach (var sourceNode in nodeInfo.Incoming)
        {
            if (_nodes.TryGetValue(sourceNode, out var sourceInfo) &&
                sourceInfo.Outgoing.TryGetValue(node, out var edge))
            {
                result.Add((sourceNode, edge));
            }
        }

        return result;
    }

    private void ApplyAddNode(TNode node) => _nodes.TryAdd(node, new NodeInfo());

    private void ApplyRemoveNode(TNode node)
    {
        if (!_nodes.Remove(node, out var nodeInfo))
        {
            return;
        }

        foreach (var destination in nodeInfo.Outgoing.Keys)
        {
            if (_nodes.TryGetValue(destination, out var destinationInfo))
            {
                destinationInfo.Incoming.Remove(node);
            }
        }

        foreach (var source in nodeInfo.Incoming)
        {
            if (_nodes.TryGetValue(source, out var sourceInfo))
            {
                sourceInfo.Outgoing.Remove(node);
            }
        }
    }

    private void ApplyAddEdge(TNode source, TNode destination, TEdge edge)
    {
        if (_nodes.TryGetValue(source, out var sourceInfo) &&
            _nodes.TryGetValue(destination, out var destinationInfo))
        {
            if (sourceInfo.Outgoing.TryAdd(destination, edge))
            {
                destinationInfo.Incoming.Add(source);
            }
        }
    }

    private void ApplyUpsertEdge(TNode source, TNode destination, TEdge edge)
    {
        _nodes[source].Outgoing[destination] = edge;
        _nodes[destination].Incoming.Add(source);
    }

    private void ApplyRemoveEdge(TNode source, TNode destination)
    {
        if (_nodes.TryGetValue(source, out var sourceInfo) &&
            _nodes.TryGetValue(destination, out var destinationInfo))
        {
            if (sourceInfo.Outgoing.Remove(destination))
            {
                destinationInfo.Incoming.Remove(source);
            }
        }
    }

    private void ApplyClear() => _nodes.Clear();

    private IEnumerable<(TNode Source, TNode Destination, TEdge Edge)> GetEdges()
    {
        foreach (var (source, nodeInfo) in _nodes)
        {
            foreach (var (destination, edge) in nodeInfo.Outgoing)
            {
                yield return (source, destination, edge);
            }
        }
    }

    private JournalStreamWriter GetWriter()
    {
        Debug.Assert(_writer.HasValue);
        return _writer.Value;
    }

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    public IEnumerator<(TNode Node, IReadOnlyCollection<(TNode Destination, TEdge Edge)> OutgoingEdges)> GetEnumerator() =>
        _nodes.Select(kvp => (kvp.Key, (IReadOnlyCollection<(TNode, TEdge)>)
            kvp.Value.Outgoing.Select(e => (e.Key, e.Value)).ToList())).GetEnumerator();

    private sealed class NodeInfo
    {
        /// <summary>
        /// The Key is the destination node (TNode), and the Value is the edge data (TEdge).
        /// </summary>
        public Dictionary<TNode, TEdge> Outgoing { get; } = [];
        /// <summary>
        /// Stores a unique list of all the source nodes that have an edge leading to this one.
        /// </summary>
        /// <remarks>
        /// The edge data (TEdge) is considered to be "owned" by the source node and is stored in its Outgoing list.
        /// This way we avoid duplicating the edge data by using a HashSet for incoming edges.</remarks>
        public HashSet<TNode> Incoming { get; } = [];
    }
}

internal sealed class DurableGraphDebugView<TNode, TEdge>(IDurableGraph<TNode, TEdge> graph)
    where TNode : notnull
{
    [DebuggerBrowsable(DebuggerBrowsableState.RootHidden)]
    public DebugViewItem[] Items => graph.Nodes.Select(n => new DebugViewItem(n, graph.GetOutgoing(n))).ToArray();

    [DebuggerDisplay("{Node}, Outgoing = {Outgoing.Length}")]
    internal readonly struct DebugViewItem(TNode node, IReadOnlyCollection<(TNode Node, TEdge Edge)> outgoing)
    {
        public TNode Node { get; } = node;
        public (TNode Node, TEdge Edge)[] Outgoing { get; } = [.. outgoing];
    }
}