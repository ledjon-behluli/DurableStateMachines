using Microsoft.Extensions.DependencyInjection;

namespace DurableStateMachines.Tests;

[Collection(GlobalFixture.Name)]
public class DurableOrderedSetTests(TestFixture fixture)
{
    public interface IDurableOrderedSetGrain : IGrainWithStringKey
    {
        Task<bool> Add(string value);
        Task<bool> Remove(string value);
        Task<bool> Contains(string value);
        Task<int> GetCount();
        Task Clear();
        Task<List<string>> GetAll();
        Task<string[]> GetOrderedItemsAsArray();
        Task<TryValue<string>> TryGetValue(string value);
        Task<string[]> CopyToArray(int arraySize, int arrayIndex);
    }

    public class DurableOrderedSetGrain([FromKeyedServices("ordered_set")]
        IDurableOrderedSet<string> state) : DurableGrain, IDurableOrderedSetGrain
    {
        public async Task<bool> Add(string value)
        {
            var result = state.Add(value);
            
            if (result)
            {
                await WriteStateAsync();
            }

            return result;
        }

        public async Task<bool> Remove(string value)
        {
            var result = state.Remove(value);
            
            if (result)
            {
                await WriteStateAsync();
            }
            
            return result;
        }

        public Task<bool> Contains(string value) => Task.FromResult(state.Contains(value));

        public Task<int> GetCount() => Task.FromResult(state.Count);

        public async Task Clear()
        {
            state.Clear();
            await WriteStateAsync();
        }

        public Task<List<string>> GetAll() => Task.FromResult(state.ToList());
        public Task<string[]> GetOrderedItemsAsArray() => Task.FromResult(state.OrderedItems.ToArray());

        public Task<TryValue<string>> TryGetValue(string value)
        {
            var found = state.TryGetValue(value, out var actualValue);
            return Task.FromResult<TryValue<string>>(new(found, actualValue));
        }

        public Task<string[]> CopyToArray(int arraySize, int arrayIndex)
        {
            var array = new string[arraySize];
            state.CopyTo(array, arrayIndex);

            return Task.FromResult(array);
        }
    }

    private IDurableOrderedSetGrain GetGrain(string key) => fixture.Cluster.Client.GetGrain<IDurableOrderedSetGrain>(key);
    private static ValueTask DeactivateGrain(IDurableOrderedSetGrain grain) => grain.Cast<IGrainManagementExtension>().DeactivateOnIdle();

    [Fact]
    public async Task EmptyOperations()
    {
        var grain = GetGrain("empty");

        Assert.Equal(0, await grain.GetCount());
        Assert.False(await grain.Contains("anything"));
        Assert.False(await grain.Remove("anything"));
        Assert.Empty(await grain.GetAll());
    }

    [Fact]
    public async Task UniquenessAndOrder()
    {
        var grain = GetGrain("unique-order");
        var expectedOrder = new List<string> { "one", "two", "three" };

        Assert.True(await grain.Add("one"));
        Assert.True(await grain.Add("two"));
        Assert.True(await grain.Add("three"));

        Assert.Equal(3, await grain.GetCount());
        Assert.Equal(expectedOrder, await grain.GetAll());

        Assert.False(await grain.Add("two"));
        Assert.Equal(3, await grain.GetCount());

        Assert.True(await grain.Contains("one"));
        Assert.False(await grain.Contains("four"));

        Assert.True(await grain.Remove("two"));
        Assert.Equal(2, await grain.GetCount());
        Assert.False(await grain.Contains("two"));

        expectedOrder.Remove("two");
        Assert.Equal(expectedOrder, await grain.GetAll());

        Assert.False(await grain.Remove("four"));
        Assert.Equal(2, await grain.GetCount());
    }

    [Fact]
    public async Task OrderedItems()
    {
        var grain = GetGrain("ordered-items-span");
        Assert.Empty(await grain.GetOrderedItemsAsArray());

        var expectedOrder = new[] { "first", "second", "third" };
        foreach (var item in expectedOrder)
        {
            await grain.Add(item);
        }

        var actualItems = await grain.GetOrderedItemsAsArray();
        Assert.Equal(expectedOrder, actualItems);

        await grain.Remove("second");
        var itemsAfterRemove = await grain.GetOrderedItemsAsArray();
        Assert.Equal(["first", "third"], itemsAfterRemove);

        await grain.Clear();
        Assert.Empty(await grain.GetOrderedItemsAsArray());
    }

    [Fact]
    public async Task TryGetValue()
    {
        var grain = GetGrain("try-get-value");

        var (foundEmpty, valueEmpty) = await grain.TryGetValue("anything");
        Assert.False(foundEmpty);
        Assert.Null(valueEmpty);

        await grain.Add("one");
        await grain.Add("two");

        var (foundExisting, valueExisting) = await grain.TryGetValue("one");
        Assert.True(foundExisting);
        Assert.Equal("one", valueExisting);

        var (foundMissing, valueMissing) = await grain.TryGetValue("three");
        Assert.False(foundMissing);
        Assert.Null(valueMissing);
    }

    [Fact]
    public async Task CopyTo()
    {
        var grain = GetGrain("copy-to");
        var items = new[] { "a", "b", "c" };
        foreach (var item in items)
        {
            await grain.Add(item);
        }

        var destination1 = await grain.CopyToArray(3, 0);
        Assert.Equal(items, destination1);

        var destination2 = await grain.CopyToArray(5, 0);
        Assert.Equal(new[] { "a", "b", "c", null, null }, destination2);

        var destination3 = await grain.CopyToArray(5, 2);
        Assert.Equal(new[] { null, null, "a", "b", "c" }, destination3);

        await Assert.ThrowsAsync<ArgumentException>(() => grain.CopyToArray(2, 0));
        await Assert.ThrowsAsync<ArgumentException>(() => grain.CopyToArray(4, 2));

        await grain.Clear();
        var destination4 = await grain.CopyToArray(5, 0);
        Assert.Equal(new string[5], destination4);
    }

    [Fact]
    public async Task Persistence()
    {
        var grain = GetGrain("persist");
        var expectedOrder = new List<string> { "one", "two", "three" };

        foreach (var item in expectedOrder)
        {
            await grain.Add(item);
        }

        await DeactivateGrain(grain);

        Assert.Equal(3, await grain.GetCount());
        Assert.Equal(expectedOrder, await grain.GetAll());

        Assert.True(await grain.Remove("one"));
        Assert.Equal(2, await grain.GetCount());

        expectedOrder.Remove("one");
        Assert.Equal(expectedOrder, await grain.GetAll());
    }

    [Fact]
    public async Task Clear()
    {
        var grain = GetGrain("clear");

        await grain.Add("one");
        await grain.Add("two");
        await grain.Add("three");

        Assert.Equal(3, await grain.GetCount());

        await grain.Clear();
        Assert.Equal(0, await grain.GetCount());

        await DeactivateGrain(grain);

        Assert.Equal(0, await grain.GetCount());
        Assert.Empty(await grain.GetAll());
    }

    [Fact]
    public async Task Restore()
    {
        var grain = GetGrain("restore");

        // We push a large number of items to ensure the state machine's logic
        // will trigger a snapshot operation upon deactivation.

        const int NumItems = 100;
        var expectedOrder = Enumerable.Range(1, NumItems).Select(i => i.ToString()).ToList();

        foreach (var item in expectedOrder)
        {
            await grain.Add(item);
        }

        await DeactivateGrain(grain); // To trigger a restore from the snapshot

        Assert.Equal(NumItems, await grain.GetCount());
        Assert.Equal(expectedOrder, await grain.GetAll());

        await grain.Remove("50");
        expectedOrder.Remove("50");

        await DeactivateGrain(grain);

        Assert.Equal(NumItems - 1, await grain.GetCount());
        Assert.False(await grain.Contains("50"));
        Assert.Equal(expectedOrder, await grain.GetAll());
    }
}