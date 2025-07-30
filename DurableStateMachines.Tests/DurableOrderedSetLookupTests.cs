using Microsoft.Extensions.DependencyInjection;
using System.Collections.ObjectModel;

namespace DurableStateMachines.Tests;

[Collection(GlobalFixture.Name)]
public class DurableOrderedSetLookupTests(TestFixture fixture)
{
    public interface IDurableOrderedSetLookupGrain : IGrainWithStringKey
    {
        Task<bool> Add(string key, string value);
        Task<bool> RemoveKey(string key);
        Task<bool> RemoveItem(string key, string value);
        Task Clear();
        Task<ReadOnlyCollection<string>> GetValues(string key);
        Task<ReadOnlyCollection<string>> GetKeys();
        Task<int> GetCount();
        Task<bool> ContainsKey(string key);
        Task<bool> ContainsItem(string key, string value);
        Task<Dictionary<string, List<string>>> GetAll();
    }

    public class DurableOrderedSetLookupGrain([FromKeyedServices("ordered_set_lookup")]
        IDurableOrderedSetLookup<string, string> state) : DurableGrain, IDurableOrderedSetLookupGrain
    {
        public async Task<bool> Add(string key, string value)
        {
            var result = state.Add(key, value);
            
            if (result)
            {
                await WriteStateAsync();
            }
            
            return result;
        }

        public async Task<bool> RemoveKey(string key)
        {
            var result = state.Remove(key);
            
            if (result)
            {
                await WriteStateAsync();
            }
            
            return result;
        }

        public async Task<bool> RemoveItem(string key, string value)
        {
            var result = state.Remove(key, value);
            if (result)
            {
                await WriteStateAsync();
            }
            return result;
        }

        public async Task Clear()
        {
            state.Clear();
            await WriteStateAsync();
        }

        public Task<ReadOnlyCollection<string>> GetValues(string key) =>
          Task.FromResult(new ReadOnlyCollection<string>([.. state[key]]));

        public Task<ReadOnlyCollection<string>> GetKeys() =>
          Task.FromResult(new ReadOnlyCollection<string>([.. state.Keys]));

        public Task<int> GetCount() => Task.FromResult(state.Count);

        public Task<bool> ContainsKey(string key) => Task.FromResult(state.Contains(key));
        
        public Task<bool> ContainsItem(string key, string value) => Task.FromResult(state.Contains(key, value));

        public Task<Dictionary<string, List<string>>> GetAll()
        {
            var result = state.ToDictionary(kvp => kvp.Item1, kvp => kvp.Item2.ToList());
            return Task.FromResult(result);
        }
    }

    private IDurableOrderedSetLookupGrain GetGrain(string key) => fixture.Cluster.Client.GetGrain<IDurableOrderedSetLookupGrain>(key);
    private static ValueTask DeactivateGrain(IDurableOrderedSetLookupGrain grain) => grain.Cast<IGrainManagementExtension>().DeactivateOnIdle();

    [Fact]
    public async Task EmptyOperations()
    {
        var grain = GetGrain("empty");

        Assert.Equal(0, await grain.GetCount());
        Assert.False(await grain.ContainsKey("nonexistent"));
        Assert.False(await grain.ContainsItem("nonexistent", "value"));

        var values = await grain.GetValues("nonexistent");
        Assert.Empty(values);

        var keys = await grain.GetKeys();
        Assert.Empty(keys);

        Assert.False(await grain.RemoveKey("nonexistent"));
        Assert.False(await grain.RemoveItem("nonexistent", "value"));
    }

    [Fact]
    public async Task UniquenessAndOrder()
    {
        var grain = GetGrain("unique_order");

        Assert.True(await grain.Add("key1", "c"));
        Assert.True(await grain.Add("key1", "a"));
        Assert.True(await grain.Add("key1", "b"));

        Assert.Equal(1, await grain.GetCount());
        Assert.True(await grain.ContainsKey("key1"));

        var expectedOrder = new List<string> { "c", "a", "b" };
        Assert.Equal(expectedOrder, await grain.GetValues("key1"));

        Assert.True(await grain.ContainsItem("key1", "a"));
        Assert.False(await grain.ContainsItem("key1", "z"));

        Assert.False(await grain.Add("key1", "a"));
        Assert.Equal(1, await grain.GetCount());
        Assert.Equal(expectedOrder, await grain.GetValues("key1"));

        Assert.True(await grain.RemoveItem("key1", "a"));
        expectedOrder.Remove("a");
        Assert.Equal(expectedOrder, await grain.GetValues("key1"));

        Assert.False(await grain.RemoveItem("key1", "z"));
    }

    [Fact]
    public async Task RemoveItem_RemovesKeyWhenEmpty()
    {
        var grain = GetGrain("remove_item_empties_key");

        await grain.Add("key1", "a");
        await grain.Add("key1", "b");

        Assert.True(await grain.RemoveItem("key1", "a"));
        Assert.True(await grain.ContainsKey("key1"));
        Assert.Equal(1, await grain.GetCount());

        Assert.True(await grain.RemoveItem("key1", "b"));
        Assert.False(await grain.ContainsKey("key1"));
        Assert.Equal(0, await grain.GetCount());
    }

    [Fact]
    public async Task Persistence()
    {
        var grain = GetGrain("persist");
        var expectedOrder = new List<string> { "a", "b", "c" };

        await grain.Add("key1", "a");
        await grain.Add("key1", "b");
        await grain.Add("key2", "c");

        await DeactivateGrain(grain);

        Assert.Equal(2, await grain.GetCount());
        Assert.Equivalent(new[] { "key1", "key2" }, await grain.GetKeys());
        Assert.Equal(["a", "b"], await grain.GetValues("key1"));
        Assert.Equal(["c"], await grain.GetValues("key2"));

        await grain.RemoveItem("key1", "a");
        await DeactivateGrain(grain);

        Assert.Equal(2, await grain.GetCount());
        Assert.Equal(["b"], await grain.GetValues("key1"));
    }

    [Fact]
    public async Task Clear()
    {
        var grain = GetGrain("clear");

        await grain.Add("key1", "a");
        await grain.Add("key2", "b");
        Assert.Equal(2, await grain.GetCount());

        await grain.Clear();
        Assert.Equal(0, await grain.GetCount());
        Assert.Empty(await grain.GetKeys());

        await DeactivateGrain(grain);
        Assert.Equal(0, await grain.GetCount());
    }

    [Fact]
    public async Task Enumeration()
    {
        var grain = GetGrain("enum");

        var expected = new Dictionary<string, List<string>>
        {
            ["key1"] = ["a", "b"],
            ["key2"] = ["c"],
            ["key3"] = ["f", "e", "d"]
        };

        await grain.Add("key1", "a");
        await grain.Add("key1", "b");
        await grain.Add("key2", "c");
        await grain.Add("key3", "f");
        await grain.Add("key3", "e");
        await grain.Add("key3", "d");

        var actual = await grain.GetAll();

        Assert.Equal(expected.Count, actual.Count);
        Assert.Equivalent(expected.Keys, actual.Keys);
        Assert.Equal(expected["key1"], actual["key1"]);
        Assert.Equal(expected["key2"], actual["key2"]);
        Assert.Equal(expected["key3"], actual["key3"]);
    }

    [Fact]
    public async Task Restore()
    {
        var grain = GetGrain("restore");

        const int NumKeys = 50;
        const int NumItemsPerKey = 20;

        var expected = new Dictionary<string, List<string>>();

        // We add a large number of items to ensure the state machine's logic
        // will trigger a snapshot operation upon deactivation.

        for (int i = 0; i < NumKeys; i++)
        {
            var key = $"key{i}";
            var values = new List<string>();

            for (int j = 0; j < NumItemsPerKey; j++)
            {
                var value = $"val{i}_{j}";
                await grain.Add(key, value);
                values.Add(value);
            }

            expected.Add(key, values);
        }

        await DeactivateGrain(grain); // To trigger a restore from the snapshot

        var actualAfterRestore = await grain.GetAll();
        Assert.Equal(NumKeys, await grain.GetCount());
        Assert.Equivalent(expected.Keys, actualAfterRestore.Keys);

        foreach (var key in expected.Keys)
        {
            Assert.Equal(expected[key], actualAfterRestore[key]);
        }
    }
}