using Microsoft.Extensions.DependencyInjection;

namespace DurableStateMachines.Tests;

public class DurablePriorityQueueTests(TestFixture fixture) : IClassFixture<TestFixture>
{
    public interface IDurablePriorityQueueGrain : IGrainWithStringKey
    {
        Task Enqueue(string value, int priority);
        Task<string> Dequeue();
        Task<TryValue<string>> TryDequeue();
        Task<string> Peek();
        Task<TryValue<string>> TryPeek();
        Task<int> GetCount();
        Task Clear();
        Task<List<(string, int)>> GetAll();
    }

    public class DurablePriorityQueueGrain([FromKeyedServices("queue")] 
        IDurablePriorityQueue<string, int> state) : DurableGrain, IDurablePriorityQueueGrain
    {
        public async Task Enqueue(string value, int priority)
        {
            state.Enqueue(value, priority);
            await WriteStateAsync();
        }

        public async Task<string> Dequeue()
        {
            var v = state.Dequeue();
            await WriteStateAsync();

            return v;
        }

        public async Task<TryValue<string>> TryDequeue()
        {
            var result = state.TryDequeue(out var item, out _);
            await WriteStateAsync();

            return new(result, item);
        }

        public Task<string> Peek() => Task.FromResult(state.Peek());

        public async Task<TryValue<string>> TryPeek()
        {
            var result = state.TryPeek(out var item, out _);
            await WriteStateAsync();

            return new(result, item);
        }

        public Task<int> GetCount() => Task.FromResult(state.Count);

        public async Task Clear()
        {
            state.Clear();
            await WriteStateAsync();
        }

        public Task<List<(string, int)>> GetAll() => Task.FromResult(state.ToList());
    }

    private IDurablePriorityQueueGrain GetGrain(string key) => fixture.Cluster.Client.GetGrain<IDurablePriorityQueueGrain>(key);
    private static ValueTask DeactivateGrain(IDurablePriorityQueueGrain grain) => grain.Cast<IGrainManagementExtension>().DeactivateOnIdle();

    [Fact]
    public async Task EmptyOperations()
    {
        var grain = GetGrain("empty");

        Assert.Equal(0, await grain.GetCount());

        await Assert.ThrowsAsync<InvalidOperationException>(grain.Peek);
        await Assert.ThrowsAsync<InvalidOperationException>(grain.Dequeue);

        var tryPeek = await grain.TryPeek();
        Assert.False(tryPeek.Result);

        var tryDequeue = await grain.TryDequeue();
        Assert.False(tryDequeue.Result);
    }

    [Fact]
    public async Task BasicOperations()
    {
        var grain = GetGrain("basic");

        await grain.Enqueue("A", 3);
        await grain.Enqueue("B", 1);
        await grain.Enqueue("C", 2);

        Assert.Equal(3, await grain.GetCount());
        Assert.Equal("B", await grain.Peek());
        Assert.Equal("B", await grain.Dequeue());
       
        Assert.Equal(2, await grain.GetCount());
        Assert.Equal("C", await grain.Peek());
        Assert.Equal("C", await grain.Dequeue());

        Assert.Equal(1, await grain.GetCount());
        Assert.Equal("A", await grain.Peek());
        Assert.Equal("A", await grain.Dequeue());
    }

    [Fact]
    public async Task MixedOperations()
    {
        var grain = GetGrain("mixed");

        await grain.Enqueue("Urgent", 1);
        await grain.Enqueue("Medium", 3);
        Assert.Equal("Urgent", await grain.Dequeue());

        await grain.Enqueue("High", 2);
        await grain.Enqueue("NewUrgent", 1);

        Assert.Equal("NewUrgent", await grain.Dequeue());
        Assert.Equal("High", await grain.Dequeue());
        Assert.Equal("Medium", await grain.Dequeue());
    }

    [Fact]
    public async Task Persistence()
    {
        var grain = GetGrain("persist");

        await grain.Enqueue("Urgent", 1);
        await grain.Enqueue("Medium", 3);
        await grain.Enqueue("High", 2);

        await DeactivateGrain(grain);

        Assert.Equal(3, await grain.GetCount());
        Assert.Equal("Urgent", await grain.Dequeue());
        Assert.Equal("High", await grain.Dequeue());
        Assert.Equal("Medium", await grain.Dequeue());
    }

    [Fact]
    public async Task Clear()
    {
        var grain = GetGrain("clear");

        await grain.Enqueue("A", 1);
        await grain.Enqueue("B", 2);

        Assert.Equal(2, await grain.GetCount());

        await grain.Clear();

        Assert.Equal(0, await grain.GetCount());
    }

    [Fact]
    public async Task EqualPriorityOrder()
    {
        var grain = GetGrain("priority");

        await grain.Enqueue("First", 1);
        await grain.Enqueue("Second", 1);
        await grain.Enqueue("Third", 2);

        var first = await grain.Dequeue();
        var second = await grain.Dequeue();
        var third = await grain.Dequeue();

        Assert.Contains(first, new[] { "First", "Second" });
        Assert.Contains(second, new[] { "First", "Second" });
        Assert.NotEqual(first, second);
        Assert.Equal("Third", third);
    }

    [Fact]
    public async Task Enumeration()
    {
        var grain = GetGrain("enum");

        await grain.Enqueue("A", 3);
        await grain.Enqueue("B", 1);
        await grain.Enqueue("C", 2);

        var items = await grain.GetAll();

        Assert.Contains(("A", 3), items);
        Assert.Contains(("B", 1), items);
        Assert.Contains(("C", 2), items);
        Assert.Equal(3, items.Count);
    }

    [Fact]
    public async Task Restore()
    {
        var grain = GetGrain("restore");

        const int NumItems = 100;

        // We enqueue items with high priority first (descending from 100 down to 1).
        // This is crucial to test that the priority logic, not insertion order, is preserved.

        for (int i = NumItems; i >= 1; i--)
        {
            await grain.Enqueue(i.ToString(), i);
        }

        await DeactivateGrain(grain); // To trigger a restore from the snapshot

        // Verify that items are dequeued in correct priority order (ascending from 1 to 100).

        for (int expectedPriority = 1; expectedPriority <= NumItems; expectedPriority++)
        {
            var actualElement = await grain.Dequeue();
            Assert.Equal(expectedPriority.ToString(), actualElement);
        }

        Assert.Equal(0, await grain.GetCount());
    }
}