using Microsoft.Extensions.DependencyInjection;

namespace DurableStateMachines.Tests;

[Collection(GlobalFixture.Name)]
public class DurableRingBufferCollectionTests(TestFixture fixture)
{
    public interface IDurableRingBufferCollectionGrain : IGrainWithStringKey
    {
        // Workaround methods to survive deactivation
        Task<Dictionary<string, int>> GetAllCapacities();
        Task SetAllCapacities(Dictionary<string, int> capacities);

        // Collection methods
        Task<int> GetBuffersCount();
        Task<List<string>> GetKeys();
        Task<bool> ContainsBuffer(string key);
        Task<bool> RemoveBuffer(string key);
        Task ClearAll();

        // Buffer methods
        Task Enqueue(string key, string value);
        Task<TryValue<string?>> TryDequeue(string key);
        Task SetBufferCapacity(string key, int capacity);
        Task ClearBuffer(string key);
        Task<int> GetBufferCapacity(string key);
        Task<int> GetBufferItemsCount(string key);
        Task<List<string>> GetAllBufferItems(string key);
    }

    public class DurableRingBufferCollectionGrain(
        [FromKeyedServices("ring-buffer-collection")] IDurableRingBufferCollection<string, string> state)
            : DurableGrain, IDurableRingBufferCollectionGrain
    {
        private const int DefaultCapacity = 1;
        private readonly Dictionary<string, int> _capacities = [];

        private IDurableRingBuffer<string> GetBuffer(string key)
        {
            var capacity = _capacities.GetValueOrDefault(key, DefaultCapacity);
            return state.EnsureBuffer(key, capacity);
        }

        public Task<Dictionary<string, int>> GetAllCapacities() => Task.FromResult(new Dictionary<string, int>(_capacities));

        public Task SetAllCapacities(Dictionary<string, int> capacities)
        {
            _capacities.Clear();
            foreach (var (key, value) in capacities)
            {
                _capacities[key] = value;
            }
            return Task.CompletedTask;
        }

        public Task<int> GetBuffersCount() => Task.FromResult(state.Count);
        public Task<List<string>> GetKeys() => Task.FromResult(state.Keys.ToList());
        public Task<bool> ContainsBuffer(string key) => Task.FromResult(state.Contains(key));

        public async Task<bool> RemoveBuffer(string key)
        {
            var removed = state.Remove(key);
            if (removed)
            {
                _capacities.Remove(key);
                await WriteStateAsync();
            }
            return removed;
        }

        public async Task ClearAll()
        {
            _capacities.Clear();
            state.Clear();

            await WriteStateAsync();
        }

        public async Task Enqueue(string key, string value)
        {
            GetBuffer(key).Enqueue(value);
            await WriteStateAsync();
        }

        public async Task<TryValue<string?>> TryDequeue(string key)
        {
            var success = GetBuffer(key).TryDequeue(out var item);
            if (success)
            {
                await WriteStateAsync();
            }
            return new(success, item);
        }

        public async Task SetBufferCapacity(string key, int capacity)
        {
            _capacities[key] = capacity;
            GetBuffer(key); // This will use the now just set '_capacities[key]', as it does EnsureBuffer(key, capacity).
            await WriteStateAsync();
        }

        public async Task ClearBuffer(string key)
        {
            GetBuffer(key).Clear();
            await WriteStateAsync();
        }

        public Task<int> GetBufferCapacity(string key) => Task.FromResult(GetBuffer(key).Capacity);
        public Task<int> GetBufferItemsCount(string key) => Task.FromResult(GetBuffer(key).Count);
        public Task<List<string>> GetAllBufferItems(string key) => Task.FromResult(GetBuffer(key).ToList());
    }

    private IDurableRingBufferCollectionGrain GetGrain(string key) => fixture.Cluster.Client.GetGrain<IDurableRingBufferCollectionGrain>(key);
    private static ValueTask DeactivateGrain(IDurableRingBufferCollectionGrain grain) => grain.Cast<IGrainManagementExtension>().DeactivateOnIdle();

    [Fact]
    public async Task BasicOperations()
    {
        var grain = GetGrain("basic");

        Assert.Equal(0, await grain.GetBuffersCount());

        await grain.SetBufferCapacity("A", 10);
        Assert.Equal(1, await grain.GetBuffersCount());
        Assert.True(await grain.ContainsBuffer("A"));
        Assert.Equal(["A"], await grain.GetKeys());

        await grain.SetBufferCapacity("B", 5);
        Assert.Equal(2, await grain.GetBuffersCount());
        Assert.True(await grain.ContainsBuffer("B"));

        Assert.True(await grain.RemoveBuffer("A"));
        Assert.Equal(1, await grain.GetBuffersCount());
        Assert.False(await grain.ContainsBuffer("A"));

        Assert.False(await grain.RemoveBuffer("A"));

        await grain.ClearAll();
        Assert.Equal(0, await grain.GetBuffersCount());
        Assert.False(await grain.ContainsBuffer("B"));
    }

    [Fact]
    public async Task GetOrCreate()
    {
        var grain = GetGrain("get-or-create");

        const string keyA = "BufferA";

        // We create a buffer implicitly by setting its capacity.
        await grain.SetBufferCapacity(keyA, 10);
        Assert.Equal(10, await grain.GetBufferCapacity(keyA));

        // Than we enqueue an item. This calls GetBuffer(keyA).
        // Because the grain now remembers the capacity is 10, it will be used,
        // and the durable state's capacity will not be changed.
        await grain.Enqueue(keyA, "item1");

        // The capacity should have NOT changed.
        Assert.Equal(10, await grain.GetBufferCapacity(keyA));
        Assert.Equal(1, await grain.GetBufferItemsCount(keyA));

        // Otherwise if we explicitly set the capacity to something new, it should always work.
        await grain.SetBufferCapacity(keyA, 99);
        Assert.Equal(99, await grain.GetBufferCapacity(keyA));
    }

    [Fact]
    public async Task Isolation()
    {
        var grain = GetGrain("isolation");

        const string KeyA = "BufferA";
        const string KeyB = "BufferB";

        await grain.SetBufferCapacity(KeyA, 3);
        await grain.SetBufferCapacity(KeyB, 5);

        await grain.Enqueue(KeyA, "a1");
        Assert.Equal(1, await grain.GetBufferItemsCount(KeyA));
        Assert.Equal(0, await grain.GetBufferItemsCount(KeyB));
        Assert.Equal(["a1"], await grain.GetAllBufferItems(KeyA));
        Assert.Empty(await grain.GetAllBufferItems(KeyB));

        await grain.Enqueue(KeyB, "b1");
        await grain.Enqueue(KeyB, "b2");
        Assert.Equal(1, await grain.GetBufferItemsCount(KeyA));
        Assert.Equal(2, await grain.GetBufferItemsCount(KeyB));
        Assert.Equal(["b1", "b2"], await grain.GetAllBufferItems(KeyB));

        var (success, item) = await grain.TryDequeue(KeyA);
        Assert.True(success);
        Assert.Equal("a1", item);
        Assert.Empty(await grain.GetAllBufferItems(KeyA));
        Assert.Equal(2, await grain.GetBufferItemsCount(KeyB));

        await grain.Enqueue(KeyA, "a2"); // Put something back in "BufferA"
        await grain.ClearBuffer(KeyB);
        Assert.Equal(1, await grain.GetBufferItemsCount(KeyA));
        Assert.Equal(0, await grain.GetBufferItemsCount(KeyB));
    }

    [Fact]
    public async Task Persistence()
    {
        var grain = GetGrain("persistence");

        const string KeyA = "BufferA";
        const string KeyB = "BufferB";
        const string KeyC = "BufferC";

        await grain.SetBufferCapacity(KeyA, 10);
        await grain.SetBufferCapacity(KeyB, 10);
        await grain.SetBufferCapacity(KeyC, 10);
        await grain.Enqueue(KeyA, "a1");
        await grain.Enqueue(KeyA, "a2");
        await grain.Enqueue(KeyB, "b1");
        await grain.RemoveBuffer(KeyC);

        var capacities1 = await grain.GetAllCapacities();
        await DeactivateGrain(grain);
        await grain.SetAllCapacities(capacities1);

        Assert.Equal(2, await grain.GetBuffersCount());
        Assert.True(await grain.ContainsBuffer(KeyA));
        Assert.True(await grain.ContainsBuffer(KeyB));
        Assert.False(await grain.ContainsBuffer(KeyC));

        Assert.Equal(2, await grain.GetBufferItemsCount(KeyA));
        Assert.Equal(["a1", "a2"], await grain.GetAllBufferItems(KeyA));
        Assert.Equal(1, await grain.GetBufferItemsCount(KeyB));
        Assert.Equal(["b1"], await grain.GetAllBufferItems(KeyB));

        await grain.TryDequeue(KeyA); // Should only remove item "a1" not "BufferA"
        await grain.RemoveBuffer(KeyB);

        var capacities2 = await grain.GetAllCapacities();
        await DeactivateGrain(grain);
        await grain.SetAllCapacities(capacities2);

        Assert.Equal(1, await grain.GetBuffersCount());
        Assert.True(await grain.ContainsBuffer(KeyA));
        Assert.False(await grain.ContainsBuffer(KeyB));
        Assert.Equal(1, await grain.GetBufferItemsCount(KeyA));
        Assert.Equal(["a2"], await grain.GetAllBufferItems(KeyA));
    }

    [Fact]
    public async Task Restore()
    {
        var grain = GetGrain("restore");

        const int numBuffers = 100; // To trigger a snapshot by having many log entries

        for (var i = 0; i < numBuffers; i++)
        {
            var key = $"key-{i}";

            await grain.SetBufferCapacity(key, 5);
            await grain.Enqueue(key, $"item-{i}-1");
            await grain.Enqueue(key, $"item-{i}-2");
        }

        var capacities = await grain.GetAllCapacities();
        await DeactivateGrain(grain); // To trigger a restore from the snapshot
        await grain.SetAllCapacities(capacities);

        Assert.Equal(numBuffers, await grain.GetBuffersCount());

        // Here we check a few buffers to ensure their state was restored correctly.

        var keyFirst = "key-0";
        var keyMid = $"key-{numBuffers / 2}";
        var keyLast = $"key-{numBuffers - 1}";

        Assert.True(await grain.ContainsBuffer(keyFirst));
        Assert.Equal(2, await grain.GetBufferItemsCount(keyFirst));
        Assert.Equal([$"item-0-1", $"item-0-2"], await grain.GetAllBufferItems(keyFirst));

        Assert.True(await grain.ContainsBuffer(keyMid));
        Assert.Equal(2, await grain.GetBufferItemsCount(keyMid));
        Assert.Equal([$"item-{numBuffers / 2}-1", $"item-{numBuffers / 2}-2"], await grain.GetAllBufferItems(keyMid));

        Assert.True(await grain.ContainsBuffer(keyLast));
        Assert.Equal(2, await grain.GetBufferItemsCount(keyLast));
        Assert.Equal([$"item-{numBuffers - 1}-1", $"item-{numBuffers - 1}-2"], await grain.GetAllBufferItems(keyLast));
    }
}