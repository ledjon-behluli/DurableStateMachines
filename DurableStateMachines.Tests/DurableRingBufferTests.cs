using Microsoft.Extensions.DependencyInjection;

namespace DurableStateMachines.Tests;

[Collection(GlobalFixture.Name)]
public class DurableRingBufferTests(TestFixture fixture)
{
    public interface IDurableRingBufferGrain : IGrainWithStringKey
    {
        Task Enqueue(string value);
        Task<TryValue<string?>> TryDequeue();
        Task SetCapacity(int capacity);
        Task Clear();
        Task<int> GetCapacity();
        Task<int> GetCount();
        Task<bool> IsEmpty();
        Task<bool> IsFull();
        Task<List<string>> GetAll();
        Task<(int, string[])> CopyToArray(int arraySize, int arrayIndex);
        Task<(int, string[])> DrainToArray(int arraySize, int arrayIndex);
    }

    public class DurableRingBufferGrain(
        [FromKeyedServices("ring-buffer")] IDurableRingBuffer<string> state)
            : DurableGrain, IDurableRingBufferGrain
    {
        public async Task Enqueue(string value)
        {
            state.Enqueue(value);
            await WriteStateAsync();
        }

        public async Task<TryValue<string?>> TryDequeue()
        {
            var success = state.TryDequeue(out var result);
            if (success)
            {
                await WriteStateAsync();
            }

            return new(success, result);
        }

        public async Task SetCapacity(int capacity)
        {
            state.SetCapacity(capacity);
            await WriteStateAsync();
        }

        public async Task Clear()
        {
            state.Clear();
            await WriteStateAsync();
        }

        public Task<int> GetCapacity() => Task.FromResult(state.Capacity);
        public Task<int> GetCount() => Task.FromResult(state.Count);
        public Task<bool> IsEmpty() => Task.FromResult(state.IsEmpty);
        public Task<bool> IsFull() => Task.FromResult(state.IsFull);
        public Task<List<string>> GetAll() => Task.FromResult(state.ToList());

        public Task<(int, string[])> CopyToArray(int arraySize, int arrayIndex)
        {
            var array = new string[arraySize];
            var count = state.CopyTo(array, arrayIndex);

            return Task.FromResult((count, array));
        }

        public async Task<(int, string[])> DrainToArray(int arraySize, int arrayIndex)
        {
            var array = new string[arraySize];
            var count = state.DrainTo(array, arrayIndex);

            await WriteStateAsync();

            return (count, array);
        }
    }

    private IDurableRingBufferGrain GetGrain(string key) => fixture.Cluster.Client.GetGrain<IDurableRingBufferGrain>(key);
    private static ValueTask DeactivateGrain(IDurableRingBufferGrain grain) => grain.Cast<IGrainManagementExtension>().DeactivateOnIdle();

    [Fact]
    public async Task EmptyOperations()
    {
        var grain = GetGrain("empty");

        await grain.SetCapacity(5);

        Assert.Equal(0, await grain.GetCount());
        Assert.Equal(5, await grain.GetCapacity());
        Assert.True(await grain.IsEmpty());
        Assert.False(await grain.IsFull());

        var allItems = await grain.GetAll();

        Assert.Empty(allItems);
    }

    [Fact]
    public async Task BasicOperations()
    {
        var grain = GetGrain("basic");

        await grain.SetCapacity(3);

        await grain.Enqueue("one");
        await grain.Enqueue("two");

        Assert.Equal(2, await grain.GetCount());
        Assert.False(await grain.IsFull());

        var items1 = await grain.GetAll();
        Assert.Equal("one", items1.First());
        Assert.Equal("two", items1.Last());

        await grain.Enqueue("three");

        Assert.Equal(3, await grain.GetCount());
        Assert.True(await grain.IsFull());

        var items2 = await grain.GetAll();
        Assert.Equal("one", items2.First());
        Assert.Equal("three", items2.Last());
    }

    [Fact]
    public async Task TryDequeueRemovesOldestItem()
    {
        var grain = GetGrain("try-dequeue");
        await grain.SetCapacity(3);

        var (success1, item1) = await grain.TryDequeue();

        Assert.False(success1);
        Assert.Null(item1);

        await grain.Enqueue("one");
        await grain.Enqueue("two");
        var (success2, item2) = await grain.TryDequeue();

        Assert.True(success2);
        Assert.Equal("one", item2);
        Assert.Equal(1, await grain.GetCount());
        Assert.Equal(["two"], await grain.GetAll());

        await DeactivateGrain(grain);

        Assert.Equal(1, await grain.GetCount());
        Assert.Equal(["two"], await grain.GetAll());

        var (success3, item3) = await grain.TryDequeue();
        Assert.True(success3);
        Assert.Equal("two", item3);
        Assert.True(await grain.IsEmpty());

        var (success4, item4) = await grain.TryDequeue();
        Assert.False(success4);
        Assert.Null(item4);
    }

    [Fact]
    public async Task Overwriting()
    {
        var grain = GetGrain("overwrite");

        await grain.SetCapacity(3);
        await grain.Enqueue("one");
        await grain.Enqueue("two");
        await grain.Enqueue("three");

        Assert.Equal(new[] { "one", "two", "three" }, await grain.GetAll());

        await grain.Enqueue("four"); // This should overwrite the oldest ("one")

        Assert.Equal(3, await grain.GetCount());
        Assert.True(await grain.IsFull());

        var items1 = await grain.GetAll();
        Assert.Equal(new[] { "two", "three", "four" }, items1);
        Assert.Equal("two", items1.First());
        Assert.Equal("four", items1.Last());

        await grain.Enqueue("five");
        var items2 = await grain.GetAll();

        Assert.Equal(new[] { "three", "four", "five" }, items2);
    }

    [Fact]
    public async Task CapacityChanges()
    {
        var grain = GetGrain("capacity");

        await grain.SetCapacity(3);

        await grain.Enqueue("1");
        await grain.Enqueue("2");
        await grain.Enqueue("3");

        await grain.SetCapacity(5);

        Assert.Equal(3, await grain.GetCount());
        Assert.Equal(5, await grain.GetCapacity());
        Assert.Equal(new[] { "1", "2", "3" }, await grain.GetAll());

        // Decreasing the capacity means the oldest items should get discarded.

        await grain.Enqueue("4");
        await grain.Enqueue("5");

        await grain.SetCapacity(2);

        Assert.Equal(2, await grain.GetCount());
        Assert.Equal(2, await grain.GetCapacity());
        Assert.Equal(new[] { "4", "5" }, await grain.GetAll());
    }

    [Fact]
    public async Task Clear()
    {
        var grain = GetGrain("clear");

        await grain.SetCapacity(3);

        await grain.Enqueue("one");
        await grain.Enqueue("two");
        Assert.Equal(2, await grain.GetCount());

        await grain.Clear();
        Assert.Equal(0, await grain.GetCount());
        Assert.True(await grain.IsEmpty());
    }

    [Fact]
    public async Task CopyTo()
    {
        var grain = GetGrain("copy-to");

        await grain.SetCapacity(5);

        await grain.Enqueue("a");
        await grain.Enqueue("b");
        await grain.Enqueue("c");

        var (count1, dest1) = await grain.CopyToArray(3, 0);
        Assert.Equal(3, count1);
        Assert.Equal(new[] { "a", "b", "c" }, dest1);

        var (count2, dest2) = await grain.CopyToArray(5, 1);

        Assert.Equal(3, count2);
        Assert.Equal(new[] { null, "a", "b", "c", null }, dest2);
        Assert.Equal(3, await grain.GetCount()); // Ensuring buffer was not modified
    }

    [Fact]
    public async Task DrainTo()
    {
        var grain = GetGrain("drain-to");
        await grain.SetCapacity(5);
        await grain.Enqueue("a");
        await grain.Enqueue("b");
        await grain.Enqueue("c");

        var (count, dest) = await grain.DrainToArray(5, 0);

        Assert.Equal(3, count);
        Assert.Equal(new[] { "a", "b", "c", null, null }, dest);

        // Ensuring buffer was cleared
        Assert.Equal(0, await grain.GetCount());
        Assert.True(await grain.IsEmpty());
    }

    [Fact]
    public async Task Persistence()
    {
        var grain = GetGrain("persist");

        await grain.SetCapacity(5);

        await grain.Enqueue("one");
        await grain.Enqueue("two");
        await grain.Enqueue("three");

        await DeactivateGrain(grain);

        Assert.Equal(5, await grain.GetCapacity());
        Assert.Equal(3, await grain.GetCount());

        var items = await grain.GetAll();
        Assert.Equal(new[] { "one", "two", "three" }, items);
        Assert.Equal("one", items.First());
        Assert.Equal("three", items.Last());
    }

    [Fact]
    public async Task Enumeration()
    {
        var grain = GetGrain("enum");
        await grain.SetCapacity(5);
        var expected = new List<string> { "one", "two", "three" }; // Should enumerate from oldest to newest (FIFO)

        foreach (var v in expected)
        {
            await grain.Enqueue(v);
        }

        var actual = await grain.GetAll();
        Assert.Equal(expected, actual);
    }

    [Fact]
    public async Task SimpleRestore()
    {
        var grain = GetGrain("simple-restore");

        const int NumItems = 100;
        await grain.SetCapacity(NumItems);

        // We add a large number of items to ensure the state machine's logic
        // will trigger a snapshot operation upon deactivation.

        var expected = new List<string>();
        for (int i = 1; i <= NumItems; i++)
        {
            var item = i.ToString();

            await grain.Enqueue(item);

            expected.Add(item);
        }

        await DeactivateGrain(grain); // Trigger a restore from the snapshot

        // Here we check the buffer's FIFO integrity is preserved after restoration.
        Assert.Equal(NumItems, await grain.GetCount());
        Assert.Equal(NumItems, await grain.GetCapacity());

        var actual = await grain.GetAll();
        Assert.Equal(expected, actual);
    }

    [Fact]
    public async Task RestoreWithCapacityChanges()
    {
        var grain = GetGrain("capacity-restore");

        // We create a large buffer, fill it, and deactivate to trigger a snapshot.
        // The log will contain: [SetCapacity(100)], [Add("1")], ..., [Add("100")]
        // Upon deactivation, this is compacted into a snapshot of 100 items.

        const int InitialCapacity = 100;

        await grain.SetCapacity(InitialCapacity);

        for (int i = 1; i <= InitialCapacity; i++)
        {
            await grain.Enqueue(i.ToString());
        }

        await DeactivateGrain(grain);

        // Reactivate the grain and immediately shrink its capacity.
        // This adds a "SetCapacity(50)" command to the log *after* the snapshot.

        const int ShrunkenCapacity = 50;

        await grain.SetCapacity(ShrunkenCapacity);
        await DeactivateGrain(grain);

        // Reactivate and verify the final state.
        // The state machine will first apply the snapshot of 100 items.
        // `ApplySnapshot` reads the snapshot but only loads as many of the *newest* items as the *current* capacity allows.
        // After the snapshot is applied, it then applies the `SetCapacity(50)` command.
        // The expected result is a buffer with capacity 50, containing the *last 50* items from the snapshot.

        var finalItems = await grain.GetAll();
        var finalCapacity = await grain.GetCapacity();
        var finalCount = await grain.GetCount();

        var expectedItems = Enumerable.Range(51, 50).Select(i => i.ToString()).ToList(); // 51-100
        Assert.Equal(ShrunkenCapacity, finalCapacity);
        Assert.Equal(ShrunkenCapacity, finalCount);
        Assert.Equal(expectedItems, finalItems);

        // Now we grow the capacity and deactivate again.
        // The log now contains: [snapshot], [SetCapacity(50)], [SetCapacity(150)]

        const int GrownCapacity = 150;

        await grain.SetCapacity(GrownCapacity);
        await DeactivateGrain(grain);

        //  Reactivate and verify the final state.
        // The state machine applies the same snapshot of 100 items. 
        // It then applies `SetCapacity(50)`, reducing the items to the newest 50.
        // Finally, it applies `SetCapacity(150)`. This should resize the buffer
        // to 150 but keep the 50 items that survived the previous step.

        var grownItems = await grain.GetAll();
        var grownCap = await grain.GetCapacity();
        var grownCount = await grain.GetCount();

        Assert.Equal(GrownCapacity, grownCap);
        Assert.Equal(ShrunkenCapacity, grownCount); // Count is still 50
        Assert.Equal(expectedItems, grownItems);    // Items are still 51-100
    }
}