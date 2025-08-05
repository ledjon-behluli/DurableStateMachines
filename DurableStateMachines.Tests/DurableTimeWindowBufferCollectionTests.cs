using Microsoft.Extensions.DependencyInjection;

namespace DurableStateMachines.Tests;

[Collection(GlobalFixture.Name)]
public class DurableTimeWindowBufferCollectionTests(TestFixture fixture)
{
    public interface IDurableTimeWindowBufferCollectionGrain : IGrainWithStringKey
    {
        // Workaround methods to survive deactivation
        Task<Dictionary<string, TimeSpan>> GetAllWindows();
        Task SetAllWindows(Dictionary<string, TimeSpan> windows);

        // Collection methods
        Task<int> GetBuffersCount();
        Task<List<string>> GetKeys();
        Task<bool> ContainsBuffer(string key);
        Task<bool> RemoveBuffer(string key);
        Task ClearAll();

        // Buffer methods
        Task Enqueue(string key, string value);
        Task<TryValue<string?>> TryDequeue(string key);
        Task SetBufferWindow(string key, TimeSpan window);
        Task ClearBuffer(string key);
        Task<TimeSpan> GetBufferWindow(string key);
        Task<int> GetBufferItemsCount(string key);
        Task<List<string>> GetAllBufferItems(string key);
    }

    public class DurableTimeWindowBufferCollectionGrain(
        [FromKeyedServices("time-window-buffer-collection")] IDurableTimeWindowBufferCollection<string, string> state)
            : DurableGrain, IDurableTimeWindowBufferCollectionGrain
    {
        private static readonly TimeSpan DefaultWindow = TimeSpan.FromHours(1);
        private readonly Dictionary<string, TimeSpan> _windows = [];

        private IDurableTimeWindowBuffer<string> GetBuffer(string key)
        {
            var window = _windows.GetValueOrDefault(key, DefaultWindow);
            return state.EnsureBuffer(key, window);
        }

        public Task<Dictionary<string, TimeSpan>> GetAllWindows() => Task.FromResult(new Dictionary<string, TimeSpan>(_windows));

        public Task SetAllWindows(Dictionary<string, TimeSpan> windows)
        {
            _windows.Clear();
            foreach (var (key, value) in windows)
            {
                _windows[key] = value;
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
                _windows.Remove(key);
                await WriteStateAsync();
            }
            return removed;
        }

        public async Task ClearAll()
        {
            _windows.Clear();
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

        public async Task SetBufferWindow(string key, TimeSpan window)
        {
            _windows[key] = window;
            GetBuffer(key); // This uses EnsureBuffer, which sets the window in the durable state.
            await WriteStateAsync();
        }

        public async Task ClearBuffer(string key)
        {
            GetBuffer(key).Clear();
            await WriteStateAsync();
        }

        public Task<TimeSpan> GetBufferWindow(string key) => Task.FromResult(GetBuffer(key).Window);
        public Task<int> GetBufferItemsCount(string key) => Task.FromResult(GetBuffer(key).Count);
        public Task<List<string>> GetAllBufferItems(string key) => Task.FromResult(GetBuffer(key).ToList());
    }

    private IDurableTimeWindowBufferCollectionGrain GetGrain(string key) => fixture.Cluster.Client.GetGrain<IDurableTimeWindowBufferCollectionGrain>(key);
    private static ValueTask DeactivateGrain(IDurableTimeWindowBufferCollectionGrain grain) => grain.Cast<IGrainManagementExtension>().DeactivateOnIdle();

    [Fact]
    public async Task BasicOperations()
    {
        var grain = GetGrain("basic");

        Assert.Equal(0, await grain.GetBuffersCount());

        await grain.SetBufferWindow("A", TimeSpan.FromMinutes(10));
        Assert.Equal(1, await grain.GetBuffersCount());
        Assert.True(await grain.ContainsBuffer("A"));
        Assert.Equal(["A"], await grain.GetKeys());

        await grain.SetBufferWindow("B", TimeSpan.FromMinutes(5));
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
    public async Task EnsureBuffer()
    {
        var grain = GetGrain("ensure-buffer");

        const string KeyA = "BufferA";
        
        var window = TimeSpan.FromMinutes(10);

        await grain.SetBufferWindow(KeyA, window);
        Assert.Equal(window, await grain.GetBufferWindow(KeyA));

        await grain.Enqueue(KeyA, "item1");

        Assert.Equal(window, await grain.GetBufferWindow(KeyA));
        Assert.Equal(1, await grain.GetBufferItemsCount(KeyA));

        var newWindow = TimeSpan.FromHours(1);
        await grain.SetBufferWindow(KeyA, newWindow);
        Assert.Equal(newWindow, await grain.GetBufferWindow(KeyA));
    }

    [Fact]
    public async Task Isolation()
    {
        var grain = GetGrain("isolation");

        const string KeyA = "BufferA";
        const string KeyB = "BufferB";

        await grain.SetBufferWindow(KeyA, TimeSpan.FromSeconds(10));
        await grain.SetBufferWindow(KeyB, TimeSpan.FromSeconds(30));

        await grain.Enqueue(KeyA, "a1"); // t=0
        fixture.TimeProvider.Advance(TimeSpan.FromSeconds(5));
        await grain.Enqueue(KeyB, "b1"); // t=5

        Assert.Equal(1, await grain.GetBufferItemsCount(KeyA));
        Assert.Equal(1, await grain.GetBufferItemsCount(KeyB));
        Assert.Equal(["a1"], await grain.GetAllBufferItems(KeyA));
        Assert.Equal(["b1"], await grain.GetAllBufferItems(KeyB));

        // We advance the time so that 'a1' expires but 'b1' does not.
        // Total elapsed time = 5s + 6s = 11s.
        // 'a1' (11s old) is older than its 10s window.
        // 'b1' (6s old) is within its 30s window.
        fixture.TimeProvider.Advance(TimeSpan.FromSeconds(6));
        await grain.Enqueue(KeyA, "a2"); // This enqueue should purge old items from BufferA

        Assert.Equal(1, await grain.GetBufferItemsCount(KeyA));
        Assert.Equal(["a2"], await grain.GetAllBufferItems(KeyA));
        Assert.Equal(1, await grain.GetBufferItemsCount(KeyB));
        Assert.Equal(["b1"], await grain.GetAllBufferItems(KeyB));
    }

    [Fact]
    public async Task Persistence()
    {
        var grain = GetGrain("persistence");

        const string KeyA = "BufferA";
        const string KeyB = "BufferB";
        const string KeyC = "BufferC";

        await grain.SetBufferWindow(KeyA, TimeSpan.FromMinutes(10));
        await grain.SetBufferWindow(KeyB, TimeSpan.FromMinutes(20));
        await grain.SetBufferWindow(KeyC, TimeSpan.FromMinutes(30));
        await grain.Enqueue(KeyA, "a1");
        await grain.Enqueue(KeyA, "a2");
        await grain.Enqueue(KeyB, "b1");
        await grain.RemoveBuffer(KeyC);

        var windows1 = await grain.GetAllWindows();
        await DeactivateGrain(grain);
        await grain.SetAllWindows(windows1);

        Assert.Equal(2, await grain.GetBuffersCount());
        Assert.True(await grain.ContainsBuffer(KeyA));
        Assert.True(await grain.ContainsBuffer(KeyB));
        Assert.False(await grain.ContainsBuffer(KeyC));

        Assert.Equal(2, await grain.GetBufferItemsCount(KeyA));
        Assert.Equal(["a1", "a2"], await grain.GetAllBufferItems(KeyA));
        Assert.Equal(1, await grain.GetBufferItemsCount(KeyB));
        Assert.Equal(["b1"], await grain.GetAllBufferItems(KeyB));

        await grain.TryDequeue(KeyA); // Should only remove item "a1"
        await grain.RemoveBuffer(KeyB);

        var windows2 = await grain.GetAllWindows();
        await DeactivateGrain(grain);
        await grain.SetAllWindows(windows2);

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

        const int NumBuffers = 100; // To trigger a snapshot by having many log entries

        for (var i = 0; i < NumBuffers; i++)
        {
            var key = $"key-{i}";

            await grain.SetBufferWindow(key, TimeSpan.FromSeconds(100));
            await grain.Enqueue(key, $"item-{i}-1");

            fixture.TimeProvider.Advance(TimeSpan.FromMilliseconds(1)); // We should advance time a bit to have unique timestamps
            
            await grain.Enqueue(key, $"item-{i}-2");
        }

        var windows = await grain.GetAllWindows();
        await DeactivateGrain(grain); // To trigger a restore from the snapshot
        await grain.SetAllWindows(windows);

        Assert.Equal(NumBuffers, await grain.GetBuffersCount());

        // Here we check a few buffers to ensure their state was restored correctly.

        var firstKey = "key-0";
        var middleKey = $"key-{NumBuffers / 2}";
        var lastKey = $"key-{NumBuffers - 1}";

        Assert.True(await grain.ContainsBuffer(firstKey));
        Assert.Equal(2, await grain.GetBufferItemsCount(firstKey));
        Assert.Equal([$"item-0-1", $"item-0-2"], await grain.GetAllBufferItems(firstKey));

        Assert.True(await grain.ContainsBuffer(middleKey));
        Assert.Equal(2, await grain.GetBufferItemsCount(middleKey));
        Assert.Equal([$"item-{NumBuffers / 2}-1", $"item-{NumBuffers / 2}-2"], await grain.GetAllBufferItems(middleKey));

        Assert.True(await grain.ContainsBuffer(lastKey));
        Assert.Equal(2, await grain.GetBufferItemsCount(lastKey));
        Assert.Equal([$"item-{NumBuffers - 1}-1", $"item-{NumBuffers - 1}-2"], await grain.GetAllBufferItems(lastKey));
    }
}