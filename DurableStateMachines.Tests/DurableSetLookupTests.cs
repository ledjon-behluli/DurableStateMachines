using Microsoft.Extensions.DependencyInjection;
using System.Collections.ObjectModel;

namespace DurableStateMachines.Tests;

[Collection(GlobalFixture.Name)]
public class DurableSetLookupTests(TestFixture fixture)
{
    public interface IDurableSetLookupGrain : IGrainWithStringKey
    {
        Task<bool> Add(string key, string value);
        Task<bool> RemoveKey(string key);
        Task<bool> RemoveItem(string key, string value);
        Task Clear();
        Task<ReadOnlyCollection<string>> GetValues(string key);
        Task<ReadOnlyCollection<string>> GetKeys();
        Task<int> GetCount();
        Task<bool> Contains(string key);
        Task<Dictionary<string, List<string>>> GetAll();
    }

    public class DurableSetLookupGrain([FromKeyedServices("set_lookup")]
        IDurableSetLookup<string, string> state) : DurableGrain, IDurableSetLookupGrain
    {
        public async Task<bool> Add(string key, string value)
        {
            var result = state.Add(key, value);
            await WriteStateAsync();

            return result;
        }

        public async Task<bool> RemoveKey(string key)
        {
            var result = state.Remove(key);
            await WriteStateAsync();
            
            return result;
        }

        public async Task<bool> RemoveItem(string key, string value)
        {
            var result = state.Remove(key, value);
            await WriteStateAsync();
            
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

        public Task<bool> Contains(string key) => Task.FromResult(state.Contains(key));

        public Task<Dictionary<string, List<string>>> GetAll()
        {
            var result = state.ToDictionary(kvp => kvp.Item1, kvp => kvp.Item2.ToList());
            return Task.FromResult(result);
        }
    }

    private IDurableSetLookupGrain GetGrain(string key) => fixture.Cluster.Client.GetGrain<IDurableSetLookupGrain>(key);
    private static ValueTask DeactivateGrain(IDurableSetLookupGrain grain) => grain.Cast<IGrainManagementExtension>().DeactivateOnIdle();

    [Fact]
    public async Task EmptyOperations()
    {
        var grain = GetGrain("empty");

        Assert.Equal(0, await grain.GetCount());
        Assert.False(await grain.Contains("nonexistent"));

        var values = await grain.GetValues("nonexistent");
        Assert.Empty(values);

        var keys = await grain.GetKeys();
        Assert.Empty(keys);

        Assert.False(await grain.RemoveKey("nonexistent"));
        Assert.False(await grain.RemoveItem("nonexistent", "value"));
    }

    [Fact]
    public async Task BasicOperations()
    {
        var grain = GetGrain("basic");

        Assert.True(await grain.Add("key1", "val1_1"));
        Assert.Equal(1, await grain.GetCount());
        Assert.True(await grain.Contains("key1"));
        Assert.Equal(["val1_1"], await grain.GetValues("key1"));

        Assert.True(await grain.Add("key1", "val1_2"));
        Assert.Equal(1, await grain.GetCount());
        Assert.Equivalent(new[] { "val1_1", "val1_2" }, await grain.GetValues("key1"));

        Assert.True(await grain.Add("key2", "val2_1"));
        Assert.Equal(2, await grain.GetCount());
        Assert.Equivalent(new[] { "key1", "key2" }, await grain.GetKeys());

        var removedItem = await grain.RemoveItem("key1", "val1_1");
        Assert.True(removedItem);
        Assert.Equal(["val1_2"], await grain.GetValues("key1"));

        var removedKey = await grain.RemoveKey("key2");
        Assert.True(removedKey);
        Assert.Equal(1, await grain.GetCount());
        Assert.False(await grain.Contains("key2"));

        await grain.RemoveItem("key1", "val1_2");
        Assert.Equal(0, await grain.GetCount());
        Assert.False(await grain.Contains("key1"));
    }

    [Fact]
    public async Task IgnoresDuplicates()
    {
        var grain = GetGrain("duplicates");

        Assert.True(await grain.Add("key1", "a"));
        Assert.True(await grain.Add("key1", "b"));

        // Note: adding a duplicate value to a set should be ignored.
        Assert.False(await grain.Add("key1", "a"));
        Assert.Equal(1, await grain.GetCount());

        Assert.Equivalent(new[] { "a", "b" }, await grain.GetValues("key1"));
    }

    [Fact]
    public async Task RemoveKey()
    {
        var grain = GetGrain("remove_key");

        await grain.Add("key1", "a");
        await grain.Add("key1", "b");
        await grain.Add("key2", "c");
        await grain.Add("key3", "d");

        Assert.True(await grain.RemoveKey("key2"));
        Assert.Equal(2, await grain.GetCount());

        Assert.True(await grain.Contains("key1"));
        Assert.False(await grain.Contains("key2"));
        Assert.True(await grain.Contains("key3"));

        Assert.Equivalent(new[] { "a", "b" }, await grain.GetValues("key1"));
        Assert.Empty(await grain.GetValues("key2"));
        Assert.Equal(["d"], await grain.GetValues("key3"));

        Assert.False(await grain.RemoveKey("nonexistent"));
    }

    [Fact]
    public async Task RemoveItem()
    {
        var grain = GetGrain("remove_item");

        await grain.Add("key1", "a");
        await grain.Add("key1", "b");
        await grain.Add("key1", "c");

        Assert.True(await grain.RemoveItem("key1", "b"));
        Assert.Equivalent(new[] { "a", "c" }, await grain.GetValues("key1"));

        // Attempting to remove a value that does not exist should do nothing.
        Assert.False(await grain.RemoveItem("key1", "z"));
        Assert.Equivalent(new[] { "a", "c" }, await grain.GetValues("key1"));

        // Remove the rest of the items.
        Assert.True(await grain.RemoveItem("key1", "a"));
        Assert.True(await grain.RemoveItem("key1", "c"));

        // When the last item is removed, the key should also be removed.
        Assert.False(await grain.Contains("key1"));
        Assert.Equal(0, await grain.GetCount());
    }

    [Fact]
    public async Task Persistence()
    {
        var grain = GetGrain("persist");

        await grain.Add("key1", "a");
        await grain.Add("key1", "b");
        await grain.Add("key2", "c");

        await DeactivateGrain(grain);

        Assert.Equal(2, await grain.GetCount());
        Assert.Equivalent(new[] { "key1", "key2" }, await grain.GetKeys());
        Assert.Equivalent(new[] { "a", "b" }, await grain.GetValues("key1"));
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
    }

    [Fact]
    public async Task Enumeration()
    {
        var grain = GetGrain("enum");

        await grain.Add("key1", "a");
        await grain.Add("key1", "b");
        await grain.Add("key2", "c");
        await grain.Add("key3", "d");
        await grain.Add("key3", "e");
        await grain.Add("key3", "f");

        var actual = await grain.GetAll();

        Assert.Equivalent(new[] { "key1", "key2", "key3" }, actual.Keys);
        Assert.Equivalent(new[] { "a", "b" }, actual["key1"]);
        Assert.Equivalent(new[] { "c" }, actual["key2"]);
        Assert.Equivalent(new[] { "d", "e", "f" }, actual["key3"]);
    }

    [Fact]
    public async Task Restore()
    {
        var grain = GetGrain("restore");

        const int NumKeys = 100;
        const int NumItemsPerKey = 10;

        var expected = new Dictionary<string, HashSet<string>>();

        // We add a large number of items to ensure the state machine's logic
        // will trigger a snapshot operation upon deactivation.

        for (int i = 0; i < NumKeys; i++)
        {
            var key = $"key{i}";
            var values = new HashSet<string>();

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
            Assert.Equivalent(expected[key], actualAfterRestore[key]);
        }
    }
}