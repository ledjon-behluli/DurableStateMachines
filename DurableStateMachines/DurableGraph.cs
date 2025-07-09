using Microsoft.Extensions.DependencyInjection;
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

[DebuggerDisplay("Nodes = {Count}")]
[DebuggerTypeProxy(typeof(DurableGraphDebugView<,>))]
internal sealed class DurableGraph<TNode, TEdge> : IDurableGraph<TNode, TEdge>, IDurableStateMachine where TNode : notnull
{
    private const byte VersionByte = 0;

    private IStateMachineLogWriter? _storage;

    private readonly SerializerSessionPool _sessionPool;
    private readonly IFieldCodec<TNode> _nodeCodec;
    private readonly IFieldCodec<TEdge> _edgeCodec;

    private readonly Dictionary<TNode, NodeInfo> _nodes = [];

    public DurableGraph(
        [ServiceKey] string key, IStateMachineManager manager,
        IFieldCodec<TNode> nodeCodec, IFieldCodec<TEdge> edgeCodec,
        SerializerSessionPool sessionPool)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);

        _nodeCodec = nodeCodec;
        _edgeCodec = edgeCodec;
        _sessionPool = sessionPool;

        manager.RegisterStateMachine(key, this);
    }

    public int Count => _nodes.Count;
    public IReadOnlyCollection<TNode> Nodes => _nodes.Keys;

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
            throw new NotSupportedException($"This instance of {nameof(DurableGraph<TNode, TEdge>)} supports version {(uint)VersionByte} and not version {(uint)version}.");
        }

        var command = (CommandType)reader.ReadVarUInt32();

        switch (command)
        {
            case CommandType.AddNode: _ = ApplyAddNode(ReadNode(ref reader)); break;
            case CommandType.RemoveNode: _ = ApplyRemoveNode(ReadNode(ref reader)); break;
            case CommandType.AddEdge: _ = ApplyAddEdge(ReadNode(ref reader), ReadNode(ref reader), ReadEdge(ref reader)); break;
            case CommandType.UpsertEdge: ApplyUpsertEdge(ReadNode(ref reader), ReadNode(ref reader), ReadEdge(ref reader)); break;
            case CommandType.RemoveEdge: _ = ApplyRemoveEdge(ReadNode(ref reader), ReadNode(ref reader)); break;
            case CommandType.Clear: ApplyClear(); break;
            case CommandType.Snapshot: ApplySnapshot(ref reader); break;
            default: throw new NotSupportedException($"Command type {command} is not supported");
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        TNode ReadNode(ref Reader<ReadOnlySequenceInput> reader)
        {
            var field = reader.ReadFieldHeader();
            return _nodeCodec.ReadValue(ref reader, field);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        TEdge ReadEdge(ref Reader<ReadOnlySequenceInput> reader)
        {
            var field = reader.ReadFieldHeader();
            return _edgeCodec.ReadValue(ref reader, field);
        }

        void ApplySnapshot(ref Reader<ReadOnlySequenceInput> reader)
        {
            ApplyClear();

            // First, read the node count and reconstruct all nodes. This ensures that when we
            // process the edges in the next step, both source and destination nodes will already exist.
            var nodeCount = (int)reader.ReadVarUInt32();

            for (var i = 0; i < nodeCount; i++)
            {
                ApplyAddNode(ReadNode(ref reader));
            }

            // Next, read the edge count and reconstruct all edges,
            // linking the nodes that were created in the previous step.
            var edgeCount = (int)reader.ReadVarUInt32();

            for (var i = 0; i < edgeCount; i++)
            {
                var source = ReadNode(ref reader);
                var destination = ReadNode(ref reader);
                var edge = ReadEdge(ref reader);

                ApplyAddEdge(source, destination, edge);
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

            // The snapshot format is the list of all nodes, followed by the list of all edges.
            // This strategy avoids complex traversal logic for graphs, which can
            // contain cycles or consist of multiple, disconnected components.

            // First, write the count of all nodes, followed by each node's value.
            writer.WriteVarUInt32((uint)self._nodes.Count);

            foreach (var node in self._nodes.Keys)
            {
                self._nodeCodec.WriteField(ref writer, 0, typeof(TNode), node);
            }

            // Next, calculate and write the total number of edges.
            var edgeCount = self._nodes.Values.Sum(n => n.Outgoing.Count);
            writer.WriteVarUInt32((uint)edgeCount);

            // Finally, iterate through every outgoing edge in the graph and write it as a (source, dest, edge) triplet.
            foreach (var (source, nodeInfo) in self._nodes)
            {
                foreach (var (destination, edge) in nodeInfo.Outgoing)
                {
                    self._nodeCodec.WriteField(ref writer, 0, typeof(TNode), source);
                    self._nodeCodec.WriteField(ref writer, 1, typeof(TNode), destination);
                    self._edgeCodec.WriteField(ref writer, 2, typeof(TEdge), edge);
                }
            }

            writer.Commit();
        }, this);
    }

    public bool AddNode(TNode node)
    {
        if (ApplyAddNode(node))
        {
            GetStorage().AppendEntry(static (state, bufferWriter) =>
            {
                var (self, cmd, node) = state;

                using var session = self._sessionPool.GetSession();
                
                var writer = Writer.Create(bufferWriter, session);
                
                writer.WriteByte(VersionByte);
                writer.WriteVarUInt32((uint)cmd);
                
                self._nodeCodec.WriteField(ref writer, 0, typeof(TNode), node);
                
                writer.Commit();
            }, (this, CommandType.AddNode, node));

            return true;
        }

        return false;
    }

    public bool RemoveNode(TNode node)
    {
        if (ApplyRemoveNode(node))
        {
            GetStorage().AppendEntry(static (state, bufferWriter) =>
            {
                var (self, cmd, node) = state;
                
                using var session = self._sessionPool.GetSession();
                
                var writer = Writer.Create(bufferWriter, session);
                
                writer.WriteByte(VersionByte);
                writer.WriteVarUInt32((uint)cmd);
                
                self._nodeCodec.WriteField(ref writer, 0, typeof(TNode), node);
                
                writer.Commit();
            }, (this, CommandType.RemoveNode, node));
            
            return true;
        }

        return false;
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

        if (ApplyAddEdge(source, destination, edge))
        {
            GetStorage().AppendEntry(static (state, bufferWriter) =>
            {
                var (self, cmd, src, dest, edge) = state;
                
                using var session = self._sessionPool.GetSession();
                
                var writer = Writer.Create(bufferWriter, session);
                
                writer.WriteByte(VersionByte);
                writer.WriteVarUInt32((uint)cmd);
                
                self._nodeCodec.WriteField(ref writer, 0, typeof(TNode), src);
                self._nodeCodec.WriteField(ref writer, 1, typeof(TNode), dest);
                self._edgeCodec.WriteField(ref writer, 2, typeof(TEdge), edge);
                
                writer.Commit();
            }, (this, CommandType.AddEdge, source, destination, edge));

            return true;
        }

        return false;
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

        // If the edge does not exist, it is created.
        // If the edge exists with a different value, it is updated.
        // If the edge exists with the exact same value, it is overwritten with that same value.

        ApplyUpsertEdge(source, destination, edge);
        GetStorage().AppendEntry(static (state, bufferWriter) =>
        {
            var (self, cmd, src, dest, edge) = state;
            
            using var session = self._sessionPool.GetSession();
            
            var writer = Writer.Create(bufferWriter, session);
            
            writer.WriteByte(VersionByte);
            writer.WriteVarUInt32((uint)cmd);
            
            self._nodeCodec.WriteField(ref writer, 0, typeof(TNode), src);
            self._nodeCodec.WriteField(ref writer, 1, typeof(TNode), dest);
            self._edgeCodec.WriteField(ref writer, 2, typeof(TEdge), edge);
            
            writer.Commit();
        }, (this, CommandType.UpsertEdge, source, destination, edge));
    }

    public bool RemoveEdge(TNode source, TNode destination)
    {
        if (ApplyRemoveEdge(source, destination))
        {
            GetStorage().AppendEntry(static (state, bufferWriter) =>
            {
                var (self, cmd, src, dest) = state;
                
                using var session = self._sessionPool.GetSession();
                
                var writer = Writer.Create(bufferWriter, session);
                
                writer.WriteByte(VersionByte);
                writer.WriteVarUInt32((uint)cmd);
                
                self._nodeCodec.WriteField(ref writer, 0, typeof(TNode), src);
                self._nodeCodec.WriteField(ref writer, 1, typeof(TNode), dest);
                
                writer.Commit();
            }, (this, CommandType.RemoveEdge, source, destination));
         
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

    private bool ApplyAddNode(TNode node) => _nodes.TryAdd(node, new NodeInfo());

    private bool ApplyRemoveNode(TNode node)
    {
        if (!_nodes.Remove(node, out var nodeInfo))
        {
            return false;
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

        return true;
    }

    private bool ApplyAddEdge(TNode source, TNode destination, TEdge edge)
    {
        if (_nodes.TryGetValue(source, out var sourceInfo) &&
            _nodes.TryGetValue(destination, out var destinationInfo))
        {
            if (sourceInfo.Outgoing.TryAdd(destination, edge))
            {
                destinationInfo.Incoming.Add(source);
                return true;
            }
        }

        return false;
    }

    private void ApplyUpsertEdge(TNode source, TNode destination, TEdge edge)
    {
        _nodes[source].Outgoing[destination] = edge;
        _nodes[destination].Incoming.Add(source);
    }

    private bool ApplyRemoveEdge(TNode source, TNode destination)
    {
        if (_nodes.TryGetValue(source, out var sourceInfo) &&
            _nodes.TryGetValue(destination, out var destinationInfo))
        {
            if (sourceInfo.Outgoing.Remove(destination))
            {
                destinationInfo.Incoming.Remove(source);
                return true;
            }
        }

        return false;
    }

    private void ApplyClear() => _nodes.Clear();

    private IStateMachineLogWriter GetStorage()
    {
        Debug.Assert(_storage is not null);
        return _storage;
    }

    public IDurableStateMachine DeepCopy() => throw new NotImplementedException();

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    public IEnumerator<(TNode Node, IReadOnlyCollection<(TNode Destination, TEdge Edge)> OutgoingEdges)> GetEnumerator() =>
        _nodes.Select(kvp => (kvp.Key, (IReadOnlyCollection<(TNode, TEdge)>)
            kvp.Value.Outgoing.Select(e => (e.Key, e.Value)).ToList())).GetEnumerator();

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