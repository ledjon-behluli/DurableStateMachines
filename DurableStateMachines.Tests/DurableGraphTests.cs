using Microsoft.Extensions.DependencyInjection;
using System.Collections.ObjectModel;

namespace DurableStateMachines.Tests;

[Collection(GlobalFixture.Name)]
public class DurableGraphTests(TestFixture fixture)
{
    public interface IDurableGraphGrain : IGrainWithStringKey
    {
        Task<bool> AddNode(string node);
        Task<bool> RemoveNode(string node);
        Task<bool> AddEdge(string source, string destination, int edge);
        Task UpsertEdge(string source, string destination, int edge);
        Task<bool> RemoveEdge(string source, string destination);
        Task Clear();
        Task<int> GetNodeCount();
        Task<bool> ContainsNode(string node);
        Task<TryValue<int>> TryGetEdge(string source, string destination);
        Task<ReadOnlyCollection<string>> GetNeighbors(string node);
        Task<ReadOnlyCollection<(string, int)>> GetOutgoing(string node);
        Task<ReadOnlyCollection<(string, int)>> GetIncoming(string node);
        Task<Dictionary<string, List<(string, int)>>> GetGraphStructure();
    }

    public class DurableGraphGrain([FromKeyedServices("graph")] 
        IDurableGraph<string, int> state) : DurableGrain, IDurableGraphGrain
    {
        public async Task<bool> AddNode(string node)
        {
            var result = state.AddNode(node);
            await WriteStateAsync();

            return result;
        }

        public async Task<bool> RemoveNode(string node)
        {
            var result = state.RemoveNode(node);
            await WriteStateAsync();

            return result;
        }

        public async Task<bool> AddEdge(string source, string destination, int edge)
        {
            var result = state.AddEdge(source, destination, edge);
            await WriteStateAsync();

            return result;
        }

        public async Task UpsertEdge(string source, string destination, int edge)
        {
            state.UpsertEdge(source, destination, edge);
            await WriteStateAsync();
        }

        public async Task<bool> RemoveEdge(string source, string destination)
        {
            var result = state.RemoveEdge(source, destination);
            await WriteStateAsync();

            return result;
        }

        public async Task Clear()
        {
            state.Clear();
            await WriteStateAsync();
        }

        public Task<int> GetNodeCount() => Task.FromResult(state.Count);
        public Task<bool> ContainsNode(string node) => Task.FromResult(state.ContainsNode(node));

        public Task<TryValue<int>> TryGetEdge(string source, string destination)
        {
            var result = state.TryGetEdge(source, destination, out var edge);
            return Task.FromResult(new TryValue<int>(result, edge));
        }

        public Task<ReadOnlyCollection<string>> GetNeighbors(string node) 
            => Task.FromResult(new ReadOnlyCollection<string>([.. state.GetNeighbors(node)]));

        public Task<ReadOnlyCollection<(string, int)>> GetOutgoing(string node) 
            => Task.FromResult(new ReadOnlyCollection<(string, int)>(state.GetOutgoing(node).ToList()));

        public Task<ReadOnlyCollection<(string, int)>> GetIncoming(string node) 
            => Task.FromResult(new ReadOnlyCollection<(string, int)>(state.GetIncoming(node).ToList()));

        public Task<Dictionary<string, List<(string, int)>>> GetGraphStructure()
        {
            var adj = state.ToDictionary(n => n.Node, n => n.OutgoingEdges.ToList());
            return Task.FromResult(adj);
        }
    }

    private IDurableGraphGrain GetGrain(string key) => fixture.Cluster.Client.GetGrain<IDurableGraphGrain>(key);
    private static ValueTask DeactivateGrain(IDurableGraphGrain grain) => grain.Cast<IGrainManagementExtension>().DeactivateOnIdle();

    [Fact]
    public async Task EmptyOperations()
    {
        var grain = GetGrain("empty");

        Assert.Equal(0, await grain.GetNodeCount());
        Assert.False(await grain.ContainsNode("A"));
        Assert.Empty(await grain.GetNeighbors("A"));
    }

    [Fact]
    public async Task BasicNodeOperations()
    {
        var grain = GetGrain("nodes");

        Assert.True(await grain.AddNode("A"));
        Assert.True(await grain.AddNode("B"));
        Assert.False(await grain.AddNode("A")); // Already exists

        Assert.Equal(2, await grain.GetNodeCount());
        Assert.True(await grain.ContainsNode("A"));

        Assert.True(await grain.RemoveNode("B"));
        Assert.False(await grain.RemoveNode("C")); // Does not exist

        Assert.Equal(1, await grain.GetNodeCount());
        Assert.False(await grain.ContainsNode("B"));
    }

    [Fact]
    public async Task BasicEdgeOperations()
    {
        var grain = GetGrain("edges");

        await grain.AddNode("A");
        await grain.AddNode("B");
        await grain.AddNode("C");

        Assert.True(await grain.AddEdge("A", "B", 1));
        Assert.False(await grain.AddEdge("A", "B", 2)); // Already exists, should not change value

        var edge = await grain.TryGetEdge("A", "B");
        Assert.True(edge.Result);
        Assert.Equal(1, edge.Item);

        await grain.UpsertEdge("A", "B", 99);
        var upsertedEdge = await grain.TryGetEdge("A", "B");
        Assert.True(upsertedEdge.Result);
        Assert.Equal(99, upsertedEdge.Item);

        await grain.UpsertEdge("B", "C", 100);
        var newUpsertedEdge = await grain.TryGetEdge("B", "C");
        Assert.True(newUpsertedEdge.Result);
        Assert.Equal(100, newUpsertedEdge.Item);

        await Assert.ThrowsAsync<ArgumentException>(() => grain.AddEdge("A", "D", 3)); // D does not exist

        var noEdge = await grain.TryGetEdge("C", "A");
        Assert.False(noEdge.Result);

        Assert.True(await grain.RemoveEdge("A", "B"));
        Assert.False(await grain.RemoveEdge("A", "B")); // Already removed

        var removedEdge = await grain.TryGetEdge("A", "B");
        Assert.False(removedEdge.Result);
    }

    [Fact]
    public async Task Navigation()
    {
        var grain = GetGrain("navigation");

        await grain.AddNode("A");
        await grain.AddNode("B");
        await grain.AddNode("C");
        await grain.AddNode("D");

        await grain.AddEdge("A", "B", 1);
        await grain.AddEdge("A", "C", 2);
        await grain.AddEdge("B", "C", 3);
        await grain.AddEdge("D", "A", 4);

        //         (D)
        //          |
        //         [4]
        //          |
        //          v
        //         (A)
        //        /   \         
        //       /     \        
        //     [1]     [2] 
        //     /         \     
        //    /           \
        //   v             v
        //  (B)----[3]--->(C)  

        Assert.Equivalent(new[] { ("B", 1), ("C", 2) }, await grain.GetOutgoing("A"));
        Assert.Equivalent(new[] { ("C", 3) }, await grain.GetOutgoing("B"));
        Assert.Empty(await grain.GetOutgoing("C"));
        Assert.Equivalent(new[] { ("A", 4) }, await grain.GetOutgoing("D"));

        Assert.Equivalent(new[] { ("D", 4) }, await grain.GetIncoming("A"));
        Assert.Equivalent(new[] { ("A", 1) }, await grain.GetIncoming("B"));
        Assert.Equivalent(new[] { ("A", 2), ("B", 3) }, await grain.GetIncoming("C"));
        Assert.Empty(await grain.GetIncoming("D"));

        Assert.Equivalent(new[] { "B", "C", "D" }, await grain.GetNeighbors("A"));
        Assert.Equivalent(new[] { "A", "C" }, await grain.GetNeighbors("B"));
        Assert.Equivalent(new[] { "A", "B" }, await grain.GetNeighbors("C"));
        Assert.Equivalent(new[] { "A" }, await grain.GetNeighbors("D"));

        await grain.RemoveNode("A");

        Assert.False(await grain.ContainsNode("A"));
        Assert.Equal(3, await grain.GetNodeCount());

        Assert.Empty(await grain.GetIncoming("A"));
        Assert.Empty(await grain.GetIncoming("B"));
        Assert.Equivalent(new[] { ("B", 3) }, await grain.GetIncoming("C"));
        Assert.Empty(await grain.GetIncoming("D"));

        Assert.Empty(await grain.GetOutgoing("A"));
        Assert.Equivalent(new[] { ("C", 3) }, await grain.GetOutgoing("B"));
        Assert.Empty(await grain.GetOutgoing("C"));
        Assert.Empty(await grain.GetOutgoing("D"));
    }

    [Fact]
    public async Task HandlesCycles()
    {
        var grain = GetGrain("cycles");

        await grain.AddNode("A");
        await grain.AddNode("B");
        await grain.AddNode("C");

        await grain.AddEdge("A", "B", 1);
        await grain.AddEdge("B", "C", 2);
        await grain.AddEdge("C", "A", 3); // This creates a cycle.

        //        (A)
        //        / ʌ
        //       /   \         
        //      /     \        
        //    [1]     [3] 
        //    /         \      
        //   /           \
        //  v             \
        // (B)----[2]---->(C)  

        Assert.Equivalent(new[] { "B" }, (await grain.GetOutgoing("A")).Select(i => i.Item1).ToList());
        Assert.Equivalent(new[] { "C" }, (await grain.GetOutgoing("B")).Select(i => i.Item1).ToList());
        Assert.Equivalent(new[] { "A" }, (await grain.GetOutgoing("C")).Select(i => i.Item1).ToList());

        Assert.Equivalent(new[] { "C" }, (await grain.GetIncoming("A")).Select(i => i.Item1).ToList());
        Assert.Equivalent(new[] { "A" }, (await grain.GetIncoming("B")).Select(i => i.Item1).ToList());
        Assert.Equivalent(new[] { "B" }, (await grain.GetIncoming("C")).Select(i => i.Item1).ToList());

        Assert.Equivalent(new[] { "B", "C" }, await grain.GetNeighbors("A"));
        Assert.Equivalent(new[] { "A", "C" }, await grain.GetNeighbors("B"));
        Assert.Equivalent(new[] { "A", "B" }, await grain.GetNeighbors("C"));
    }

    [Fact]
    public async Task Persistence()
    {
        var grain = GetGrain("persist");

        await grain.AddNode("A");
        await grain.AddNode("B");
        await grain.AddEdge("A", "B", 1);

        await DeactivateGrain(grain);

        Assert.Equal(2, await grain.GetNodeCount());
        Assert.True(await grain.ContainsNode("B"));

        var edge = await grain.TryGetEdge("A", "B");

        Assert.True(edge.Result);
        Assert.Equal(1, edge.Item);
    }

    [Fact]
    public async Task Clear()
    {
        var grain = GetGrain("clear");

        await grain.AddNode("A");
        await grain.AddEdge("A", "A", 1);

        Assert.Equal(1, await grain.GetNodeCount());

        await grain.Clear();

        Assert.Equal(0, await grain.GetNodeCount());
        Assert.False(await grain.ContainsNode("A"));
    }

    [Fact]
    public async Task Restore()
    {
        var grain = GetGrain("restore");

        // We add a large number of items to ensure the state machine's logic
        // will trigger a snapshot operation upon deactivation.

        var expected = new Dictionary<string, List<(string, int)>>();

        const int NumNodes = 50;

        // First, populate the graph with all the nodes. This ensures that when we add
        // edges later, both the source and destination nodes are guaranteed to exist.
        for (int i = 0; i < NumNodes; i++)
        {
            var nodeName = $"N{i}";

            await grain.AddNode(nodeName);
            
            expected[nodeName] = [];
        }

        const int Multiplier = 3;

        // Next, add a significant number of edges in a deterministic but non-trivial pattern.
        // This creates a complex graph structure to robustly test the snapshot.
        for (int i = 0; i < Multiplier * NumNodes; i++)
        {
            var src = $"N{i % NumNodes}";
            var dest = $"N{(i * Multiplier + 1) % NumNodes}";

            if (src == dest)
            {
                // We skip self-looping edges to keep the test simple.
                continue;
            }

            // Try to add the edge. The AddEdge method returns false if the edge already exists.
            // We must mirror this logic in the 'expected' dictionary to keep it in sync.

            if (await grain.AddEdge(src, dest, i))
            {
                expected[src].Add((dest, i));
            }
        }

        await DeactivateGrain(grain); // To trigger a restore from the snapshot

        var actual = await grain.GetGraphStructure();

        Assert.Equal(expected.Count, actual.Count);

        // For each node, we verify its outgoing edges.
        foreach (var key in expected.Keys)
        {
            // We use 'Equivalent' because the exact order of edges is not guaranteed
            // or important, but the set of edges for each node must be identical.
            Assert.Equivalent(expected[key], actual[key]);
        }
    }
}