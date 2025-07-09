using Microsoft.Extensions.DependencyInjection;
using System.Collections.ObjectModel;

namespace DurableStateMachines.Tests;

public class DurableTreeTests(TestFixture fixture) : IClassFixture<TestFixture>
{
    public interface IDurableTreeGrain : IGrainWithStringKey
    {
        Task SetRoot(string value);
        Task Add(string parent, string value);
        Task<bool> Move(string value, string newParent);
        Task<bool> Remove(string value);
        Task Clear();
        Task<string> GetRoot();
        Task<bool> IsEmpty();
        Task<int> GetCount();
        Task<bool> Contains(string value);
        Task<TryValue<string>> TryGetParent(string value);
        Task<ReadOnlyCollection<string>> GetChildren(string value);
        Task<ReadOnlyCollection<string>> GetDescendants(string value);
        Task<Dictionary<string, string?>> GetTreeStructure();
    }

    public class DurableTreeGrain([FromKeyedServices("tree")] 
        IDurableTree<string> state) : DurableGrain, IDurableTreeGrain
    {
        public async Task SetRoot(string value)
        {
            state.SetRoot(value);
            await WriteStateAsync();
        }

        public async Task Add(string parent, string value)
        {
            state.Add(parent, value);
            await WriteStateAsync();
        }

        public async Task<bool> Move(string value, string newParent)
        {
            var result = state.Move(value, newParent);
            await WriteStateAsync();

            return result;
        }

        public async Task<bool> Remove(string value)
        {
            var result = state.Remove(value);
            await WriteStateAsync();

            return result;
        }

        public async Task Clear()
        {
            state.Clear();
            await WriteStateAsync();
        }

        public Task<string> GetRoot() => Task.FromResult(state.Root);
        public Task<bool> IsEmpty() => Task.FromResult(state.IsEmpty);
        public Task<int> GetCount() => Task.FromResult(state.Count);
        public Task<bool> Contains(string value) => Task.FromResult(state.Contains(value));

        public Task<ReadOnlyCollection<string>> GetChildren(string value)
            => Task.FromResult(new ReadOnlyCollection<string>([.. state.GetChildren(value)]));

        public Task<ReadOnlyCollection<string>> GetDescendants(string value)
            => Task.FromResult(new ReadOnlyCollection<string>([.. state.GetDescendants(value)]));

        public Task<TryValue<string>> TryGetParent(string value)
        {
            var result = state.TryGetParent(value, out var parent);
            return Task.FromResult(new TryValue<string>(result, parent));
        }

        public Task<Dictionary<string, string?>> GetTreeStructure()
        {
            var adjacency = new Dictionary<string, string?>();
            if (state.IsEmpty)
            {
                return Task.FromResult(adjacency);
            }

            var queue = new Queue<string>();
            queue.Enqueue(state.Root);
            adjacency[state.Root] = null;

            while (queue.TryDequeue(out var current))
            {
                foreach (var child in state.GetChildren(current))
                {
                    adjacency[child] = current;
                    queue.Enqueue(child);
                }
            }

            return Task.FromResult(adjacency);
        }
    }

    private IDurableTreeGrain GetGrain(string key) => fixture.Cluster.Client.GetGrain<IDurableTreeGrain>(key);
    private static ValueTask DeactivateGrain(IDurableTreeGrain grain) => grain.Cast<IGrainManagementExtension>().DeactivateOnIdle();

    [Fact]
    public async Task EmptyOperations()
    {
        var grain = GetGrain("empty");

        Assert.True(await grain.IsEmpty());
        Assert.Equal(0, await grain.GetCount());
        Assert.False(await grain.Contains("A"));
        await Assert.ThrowsAsync<InvalidOperationException>(grain.GetRoot);
    }

    [Fact]
    public async Task BasicOperations()
    {
        var grain = GetGrain("basic");

        await grain.SetRoot("A");
        Assert.False(await grain.IsEmpty());
        Assert.Equal(1, await grain.GetCount());
        Assert.Equal("A", await grain.GetRoot());

        await grain.Add("A", "B");
        await grain.Add("A", "C");
        await grain.Add("B", "D");

        // A
        // ├── B
        // │   └── D
        // └── C

        Assert.Equal(4, await grain.GetCount());
        Assert.True(await grain.Contains("D"));
        Assert.Equivalent(new[] { "B", "C"}, await grain.GetChildren("A"));
        Assert.Equivalent(new[] {"D"}, await grain.GetChildren("B"));
        Assert.Empty(await grain.GetChildren("C"));

        var parent = await grain.TryGetParent("D");
        Assert.True(parent.Result);
        Assert.Equal("B", parent.Item);

        // Remove a leaf node
        Assert.True(await grain.Remove("D"));
        Assert.Equal(3, await grain.GetCount());
        Assert.False(await grain.Contains("D"));

        // A
        // ├── B
        // └── C

        // Remove a node with children (should remove descendants)
        await grain.Add("B", "E"); // A->B->E, A->C
        Assert.True(await grain.Remove("B"));
        Assert.Equal(2, await grain.GetCount());
        Assert.False(await grain.Contains("B"));
        Assert.False(await grain.Contains("E"));
        Assert.Equivalent(new[] { "C" }, await grain.GetChildren("A"));
    }

    [Fact]
    public async Task MoveOperations()
    {
        var grain = GetGrain("move");

        await grain.SetRoot("A");
        await grain.Add("A", "B");
        await grain.Add("B", "C");
        await grain.Add("A", "D");

        // A
        // ├── B
        // │   └── C
        // └── D

        // Moving an ancestor to be a child of its descendant should throw.
        await Assert.ThrowsAsync<InvalidOperationException>(() => grain.Move("A", "C"));

        // Moving the root should throw.
        await Assert.ThrowsAsync<InvalidOperationException>(() => grain.Move("A", "D"));

        // Moving B to D, makes B the child of D, but C is moved with B too.
        Assert.True(await grain.Move("B", "D"));

        var parentOfB = await grain.TryGetParent("B");

        Assert.True(parentOfB.Result);
        Assert.Equal("D", parentOfB.Item);
        Assert.Equal(["D"], await grain.GetChildren("A"));
        Assert.Equal(["B"], await grain.GetChildren("D"));
        Assert.Equal(["C"], await grain.GetChildren("B"));

        // A
        // └── D
        //     └── B
        //         └── C
    }

    [Fact]
    public async Task GetDescendants()
    {
        var grain = GetGrain("descendants");

        await grain.SetRoot("A");
        await grain.Add("A", "B");
        await grain.Add("A", "C");
        await grain.Add("B", "D");
        await grain.Add("B", "E");
        await grain.Add("C", "F");

        // A
        // ├── B
        // │   ├── D
        // │   └── E
        // └── C
        //     └── F

        var descendantsOfA = await grain.GetDescendants("A");
        Assert.Equivalent(new[] { "B", "C", "D", "E", "F"}, descendantsOfA);

        var descendantsOfB = await grain.GetDescendants("B");
        Assert.Equivalent(new[] { "D", "E"}, descendantsOfB);

        var descendantsOfF = await grain.GetDescendants("F");
        Assert.Empty(descendantsOfF);
    }

    [Fact]
    public async Task Persistence()
    {
        var grain = GetGrain("persist");

        await grain.SetRoot("A");
        await grain.Add("A", "B");
        await grain.Add("A", "C");

        await DeactivateGrain(grain);

        Assert.Equal("A", await grain.GetRoot());
        Assert.Equal(3, await grain.GetCount());
        Assert.Equivalent(new[] { "B", "C" }, await grain.GetChildren("A"));
    }

    [Fact]
    public async Task Clear()
    {
        var grain = GetGrain("clear");

        await grain.SetRoot("A");
        await grain.Add("A", "B");

        Assert.Equal(2, await grain.GetCount());

        await grain.Clear();
        Assert.Equal(0, await grain.GetCount());
        Assert.True(await grain.IsEmpty());
    }

    [Fact]
    public async Task Restore()
    {
        var grain = GetGrain("restore");

        // We add a large number of items to ensure the state machine's logic
        // will trigger a snapshot operation upon deactivation.

        var expected = new Dictionary<string, string?>();
        var root = "N0";

        await grain.SetRoot(root);
        
        expected[root] = null; // Root has no parent

        // We build a tree of 100 nodes using a ternary structure.
        // Each node becomes the parent of its next 3 children.

        // N0
        // ├── N1
        // ├── N2
        // └── N3
        //     ├── N4
        //     │   ├── N7
        //     │   ├── N8
        //     │   └── N9
        //     ├── N5
        //     │   ├── N10
        //     │   ├── N11
        //     │   └── N12
        //     └── N6
        //         ├── N13
        //         ├── N14
        //         └── N15
        // ...

        for (var i = 1; i < 100; i++)
        {
            var parent = $"N{(i - 1) / 3}";
            var child = $"N{i}";

            await grain.Add(parent, child);
            
            expected[child] = parent;
        }

        await DeactivateGrain(grain); // To trigger a restore from the snapshot

        var actual = await grain.GetTreeStructure();
        Assert.Equal(expected, actual);
    }
}