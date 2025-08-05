using Microsoft.Extensions.DependencyInjection;

namespace DurableStateMachines.Tests;

[Collection(GlobalFixture.Name)]
public class DurableTimeWindowBufferTests(TestFixture fixture)
{
    public interface IDurableTimeWindowBufferGrain : IGrainWithStringKey
    {
        Task Enqueue(string value);
        Task<TryValue<string?>> TryDequeue();
        Task SetWindow(TimeSpan window);
        Task Clear();
        Task<TimeSpan> GetWindow();
        Task<int> GetCount();
        Task<bool> IsEmpty();
        Task<List<string>> GetAll();
        Task<(int, string[])> CopyToArray(int arraySize, int arrayIndex);
        Task<(int, string[])> DrainToArray(int arraySize, int arrayIndex);
    }

    public class DurableTimeWindowBufferGrain(
        [FromKeyedServices("time-window-buffer")] IDurableTimeWindowBuffer<string> state)
            : DurableGrain, IDurableTimeWindowBufferGrain
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

        public async Task SetWindow(TimeSpan window)
        {
            state.SetWindow(window);
            await WriteStateAsync();
        }

        public async Task Clear()
        {
            state.Clear();
            await WriteStateAsync();
        }

        public Task<TimeSpan> GetWindow() => Task.FromResult(state.Window);
        public Task<int> GetCount() => Task.FromResult(state.Count);
        public Task<bool> IsEmpty() => Task.FromResult(state.IsEmpty);
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

    private IDurableTimeWindowBufferGrain GetGrain(string key) => fixture.Cluster.Client.GetGrain<IDurableTimeWindowBufferGrain>(key);
    private static ValueTask DeactivateGrain(IDurableTimeWindowBufferGrain grain) => grain.Cast<IGrainManagementExtension>().DeactivateOnIdle();

    [Fact]
    public async Task EmptyOperations()
    {
        var grain = GetGrain("empty");
        var window = TimeSpan.FromMinutes(5);

        await grain.SetWindow(window);

        Assert.Equal(0, await grain.GetCount());
        Assert.Equal(window, await grain.GetWindow());
        Assert.True(await grain.IsEmpty());

        var allItems = await grain.GetAll();
        Assert.Empty(allItems);
    }

    [Fact]
    public async Task BasicOperations()
    {
        var grain = GetGrain("basic");

        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => grain.SetWindow(TimeSpan.Zero));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => grain.SetWindow(TimeSpan.FromSeconds(-1)));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => grain.SetWindow(TimeSpan.FromSeconds(0.9999)));

        await grain.SetWindow(TimeSpan.FromMinutes(1));

        await grain.Enqueue("one");       
        fixture.TimeProvider.Advance(TimeSpan.FromSeconds(10));       
        await grain.Enqueue("two");

        Assert.Equal(2, await grain.GetCount());

        var items1 = await grain.GetAll();
        Assert.Equal("one", items1.First());
        Assert.Equal("two", items1.Last());
    }

    [Fact]
    public async Task TryDequeueRemovesOldestItem()
    {
        var grain = GetGrain("try-dequeue");
        await grain.SetWindow(TimeSpan.FromMinutes(1));

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
    public async Task PurgingOldItems()
    {
        var grain = GetGrain("purge");
        await grain.SetWindow(TimeSpan.FromSeconds(10));

        await grain.Enqueue("one"); // Added at t=0
        fixture.TimeProvider.Advance(TimeSpan.FromSeconds(5));
        await grain.Enqueue("two"); // Added at t=5

        Assert.Equal(new[] { "one", "two" }, await grain.GetAll());

        fixture.TimeProvider.Advance(TimeSpan.FromSeconds(6)); // Time is now t=11, "one" is 11s old, "two" is 6s old.

        await grain.Enqueue("three"); // Enqueue triggers purge, "one" should be removed.

        Assert.Equal(2, await grain.GetCount());
        var items1 = await grain.GetAll();
        Assert.Equal(new[] { "two", "three" }, items1);
    }

    [Fact]
    public async Task WindowChanges()
    {
        var grain = GetGrain("window-changes");
        await grain.SetWindow(TimeSpan.FromSeconds(30));

        await grain.Enqueue("one"); // t=0
        fixture.TimeProvider.Advance(TimeSpan.FromSeconds(10));
        await grain.Enqueue("two"); // t=10
        fixture.TimeProvider.Advance(TimeSpan.FromSeconds(10));
        await grain.Enqueue("three"); // t=20

        // At t=20, all items are within the 30s window.
        Assert.Equal(3, await grain.GetCount());

        // Decreasing the window should purge older items immediately.
        // "one" is [20]s old, "two" is [10]s old, "three" is [0]s old.
        // New window of 15s should purge "one".
        await grain.SetWindow(TimeSpan.FromSeconds(15));

        Assert.Equal(2, await grain.GetCount());
        Assert.Equal(new[] { "two", "three" }, await grain.GetAll());
    }

    [Fact]
    public async Task Clear()
    {
        var grain = GetGrain("clear");
        await grain.SetWindow(TimeSpan.FromMinutes(1));

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
        await grain.SetWindow(TimeSpan.FromMinutes(1));

        await grain.Enqueue("one");
        await grain.Enqueue("two");
        await grain.Enqueue("three");

        var (count1, dest1) = await grain.CopyToArray(3, 0);
        Assert.Equal(3, count1);
        Assert.Equal(new[] { "one", "two", "three" }, dest1);

        var (count2, dest2) = await grain.CopyToArray(5, 1);

        Assert.Equal(3, count2);
        Assert.Equal(new[] { null, "one", "two", "three", null }, dest2);
        Assert.Equal(3, await grain.GetCount()); // Ensuring buffer was not modified
    }

    [Fact]
    public async Task DrainTo()
    {
        var grain = GetGrain("drain-to");

        await grain.SetWindow(TimeSpan.FromMinutes(1));

        await grain.Enqueue("one");
        await grain.Enqueue("two");
        await grain.Enqueue("three");

        var (count, dest) = await grain.DrainToArray(5, 0);

        Assert.Equal(3, count);
        Assert.Equal(new[] { "one", "two", "three", null, null }, dest);

        Assert.Equal(0, await grain.GetCount());
        Assert.True(await grain.IsEmpty());
    }

    [Fact]
    public async Task Persistence()
    {
        var grain = GetGrain("persist");

        var window = TimeSpan.FromMinutes(5);

        await grain.SetWindow(window);

        await grain.Enqueue("one");
        await grain.Enqueue("two");
        await grain.Enqueue("three");

        await DeactivateGrain(grain);

        Assert.Equal(window, await grain.GetWindow());
        Assert.Equal(3, await grain.GetCount());

        var items = await grain.GetAll();
        Assert.Equal(new[] { "one", "two", "three" }, items);
    }

    [Fact]
    public async Task Enumeration()
    {
        var grain = GetGrain("enum");

        await grain.SetWindow(TimeSpan.FromMinutes(1));
        var expected = new List<string> { "one", "two", "three" }; // Should enumerate in FIFO order (oldest to newest).

        foreach (var item in expected)
        {
            await grain.Enqueue(item);
        }

        var actual = await grain.GetAll();
        Assert.Equal(expected, actual);
    }

    [Fact]
    public async Task SimpleRestore()
    {
        var grain = GetGrain("simple-restore");
        var window = TimeSpan.FromHours(1);

        const int NumItems = 100;
        await grain.SetWindow(window);

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

        Assert.Equal(NumItems, await grain.GetCount());
        Assert.Equal(window, await grain.GetWindow());

        var actual = await grain.GetAll();
        Assert.Equal(expected, actual);
    }

    [Fact]
    public async Task RestoreWithWindowChanges()
    {
        var grain = GetGrain("window-restore");

        // We create a buffer with a large window, fill it, and deactivate to trigger a snapshot.
        var initialWindow = TimeSpan.FromSeconds(100);
        await grain.SetWindow(initialWindow);

        for (int i = 1; i <= 100; i++)
        {
            await grain.Enqueue(i.ToString());
            fixture.TimeProvider.Advance(TimeSpan.FromSeconds(1)); // Time is now t=100
        }

        await DeactivateGrain(grain); // Creates a snapshot with 100 items

        // Reactivate and immediately shrink the window.
        // This adds a "SetWindow(50s)" command to the log *after* the snapshot.
        var shrunkenWindow = TimeSpan.FromSeconds(50);
        await grain.SetWindow(shrunkenWindow);
        await DeactivateGrain(grain);

        // Reactivate, the state machine will apply the snapshot,
        // then the SetWindow command, which purges items older than 50s
        // relative to the current time (t=100). This leaves the last 50 items.
        var finalItems = await grain.GetAll();
        var finalWindow = await grain.GetWindow();
        var finalCount = await grain.GetCount();

        var expectedItems = Enumerable.Range(51, 50).Select(i => i.ToString()).ToList(); // Items 51-100
        Assert.Equal(shrunkenWindow, finalWindow);
        Assert.Equal(50, finalCount);
        Assert.Equal(expectedItems, finalItems);

        // Now we grow the window and deactivate again.
        // The log now contains: [snapshot], [SetWindow(50s)], [SetWindow(150s)]
        var grownWindow = TimeSpan.FromSeconds(150);
        await grain.SetWindow(grownWindow);
        await DeactivateGrain(grain);

        // Reactivate and verify the final state.
        // The state machine re-applies the log. After the SetWindow(50s) command,
        // only 50 items remain. The subsequent SetWindow(150s) command
        // only expands the window; it does not add back any items.
        var grownItems = await grain.GetAll();
        var grownWindowActual = await grain.GetWindow();
        var grownCount = await grain.GetCount();

        Assert.Equal(grownWindow, grownWindowActual);
        Assert.Equal(50, grownCount); // Count is still 50
        Assert.Equal(expectedItems, grownItems); // Items are still 51-100
    }
}