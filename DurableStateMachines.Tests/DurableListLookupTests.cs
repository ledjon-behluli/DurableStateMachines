using Microsoft.Extensions.DependencyInjection;
using System.Collections.ObjectModel;

namespace DurableStateMachines.Tests;

[Collection(GlobalFixture.Name)]
public class DurableListLookupTests(TestFixture fixture)
{
    public interface IDurableListLookupGrain : IGrainWithStringKey
    {
        Task Add(string key, string value);
        Task AddRange(string key, List<string> values);
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

    public class DurableListLookupGrain([FromKeyedServices("list_lookup")] 
        IDurableListLookup<string, string> state) : DurableGrain, IDurableListLookupGrain
    {
        public async Task Add(string key, string value)
        {
            state.Add(key, value);
            await WriteStateAsync();
        }

        public async Task AddRange(string key, List<string> values)
        {
            state.AddRange(key, values);
            await WriteStateAsync();
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

        public Task<bool> ContainsKey(string key) => Task.FromResult(state.Contains(key));

        public Task<bool> ContainsItem(string key, string value) => Task.FromResult(state.Contains(key, value));

        public Task<Dictionary<string, List<string>>> GetAll()
        {
            var result = state.ToDictionary(kvp => kvp.Item1, kvp => kvp.Item2.ToList());
            return Task.FromResult(result);
        }
    }

    private IDurableListLookupGrain GetGrain(string key) => fixture.Cluster.Client.GetGrain<IDurableListLookupGrain>(key);
    private static ValueTask DeactivateGrain(IDurableListLookupGrain grain) => grain.Cast<IGrainManagementExtension>().DeactivateOnIdle();

    [Fact]
    public async Task EmptyOperations()
    {
        var grain = GetGrain("empty");

        Assert.Equal(0, await grain.GetCount());
        Assert.False(await grain.ContainsKey("nonexistent"));

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

        await grain.Add("key1", "val1_1");
        Assert.Equal(1, await grain.GetCount());
        Assert.True(await grain.ContainsKey("key1"));
        Assert.True(await grain.ContainsItem("key1", "val1_1"));
        Assert.False(await grain.ContainsItem("key1", "nonexistent"));
        Assert.Equal(["val1_1"], await grain.GetValues("key1"));

        await grain.Add("key1", "val1_2");
        Assert.Equal(1, await grain.GetCount());
        Assert.True(await grain.ContainsItem("key1", "val1_2"));
        Assert.Equal(["val1_1", "val1_2"], await grain.GetValues("key1"));

        await grain.Add("key2", "val2_1");
        Assert.Equal(2, await grain.GetCount());
        Assert.Equivalent(new[] { "key1", "key2" }, await grain.GetKeys());

        var removedItem = await grain.RemoveItem("key1", "val1_1");
        Assert.True(removedItem);
        Assert.False(await grain.ContainsItem("key1", "val1_1"));
        Assert.Equal(["val1_2"], await grain.GetValues("key1"));

        var removedKey = await grain.RemoveKey("key2");
        Assert.True(removedKey);
        Assert.Equal(1, await grain.GetCount());
        Assert.False(await grain.ContainsKey("key2"));

        await grain.RemoveItem("key1", "val1_2");
        Assert.Equal(0, await grain.GetCount());
        Assert.False(await grain.ContainsKey("key1"));
    }

    [Fact]
    public async Task AddRangeOperation()
    {
        var grain = GetGrain("add_range");

        await grain.AddRange("key1", ["a", "b", "c"]);
        Assert.Equal(1, await grain.GetCount());
        Assert.Equal(["a", "b", "c"], await grain.GetValues("key1"));

        await grain.AddRange("key1", ["d", "e"]);
        Assert.Equal(["a", "b", "c", "d", "e"], await grain.GetValues("key1"));
    }

    [Fact]
    public async Task HandlesDuplicates()
    {
        var grain = GetGrain("duplicates");

        await grain.Add("key1", "a");
        await grain.Add("key1", "b");
        await grain.Add("key1", "a"); // Note: duplicate value
        await grain.Add("key2", "c");

        Assert.Equal(2, await grain.GetCount());

        var key1Values = await grain.GetValues("key1");
        Assert.Equal(["a", "b", "a"], key1Values);

        var key2Values = await grain.GetValues("key2");
        Assert.Equal(["c"], key2Values);
    }

    [Fact]
    public async Task RemoveKey()
    {
        var grain = GetGrain("remove_key");

        await grain.AddRange("key1", ["a", "b"]);
        await grain.AddRange("key2", ["c", "d"]);
        await grain.AddRange("key3", ["e", "f"]);

        Assert.True(await grain.RemoveKey("key2"));
        Assert.Equal(2, await grain.GetCount());

        Assert.True(await grain.ContainsKey("key1"));
        Assert.False(await grain.ContainsKey("key2"));
        Assert.True(await grain.ContainsKey("key3"));

        Assert.Equal(["a", "b"], await grain.GetValues("key1"));
        Assert.Empty(await grain.GetValues("key2"));
        Assert.Equal(["e", "f"], await grain.GetValues("key3"));

        Assert.False(await grain.RemoveKey("nonexistent"));
    }

    [Fact]
    public async Task RemoveItem()
    {
        var grain = GetGrain("remove_item");

        // Note: list only removes the first occurrence.
        await grain.AddRange("key1", ["a", "b", "c", "b"]);

        Assert.True(await grain.RemoveItem("key1", "b"));
        Assert.Equal(["a", "c", "b"], await grain.GetValues("key1"));

        // Attempting to remove a value that does not exist should do nothing.
        Assert.False(await grain.RemoveItem("key1", "z"));
        Assert.Equal(["a", "c", "b"], await grain.GetValues("key1"));

        // Remove the rest of the items.
        Assert.True(await grain.RemoveItem("key1", "a"));
        Assert.True(await grain.RemoveItem("key1", "c"));
        Assert.True(await grain.RemoveItem("key1", "b"));

        // When the last item is removed, the key should also be removed.
        Assert.False(await grain.ContainsKey("key1"));
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
    }

    [Fact]
    public async Task Enumeration()
    {
        var grain = GetGrain("enum");

        var expected = new Dictionary<string, List<string>>
        {
            ["key1"] = ["a", "b"],
            ["key2"] = ["c"],
            ["key3"] = ["d", "e", "f"]
        };

        await grain.Add("key1", "a");
        await grain.Add("key1", "b");
        await grain.Add("key2", "c");
        await grain.AddRange("key3", ["d", "e", "f"]);

        var actual = await grain.GetAll();

        Assert.Equal(expected, actual);
    }

    [Fact]
    public async Task Restore()
    {
        var grain = GetGrain("restore");

        const int NumKeys = 100;
        const int NumItemsPerKey = 10;

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
        Assert.Equal(expected, actualAfterRestore);
        Assert.Equal(NumKeys, await grain.GetCount());

        await grain.RemoveItem("key5", "val5_5");
        await DeactivateGrain(grain);

        Assert.Equal(NumKeys, await grain.GetCount());
        var key5Values = await grain.GetValues("key5");
        Assert.Equal(NumItemsPerKey - 1, key5Values.Count);
        Assert.DoesNotContain("val5_5", key5Values);
    }
}