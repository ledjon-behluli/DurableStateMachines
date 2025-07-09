using Microsoft.Extensions.DependencyInjection;

namespace DurableStateMachines.Tests;

public class DurableStackTests(TestFixture fixture) : IClassFixture<TestFixture>
{
    public interface IDurableStackGrain : IGrainWithStringKey
    {
        Task Push(string value);
        Task<string> Pop();
        Task<TryValue<string>> TryPop();
        Task<string> Peek();
        Task<TryValue<string>> TryPeek();
        Task<int> GetCount();
        Task Clear();
        Task<List<string>> GetAll();
    }

    public class DurableStackGrain([FromKeyedServices("stack")] 
        IDurableStack<string> state) : DurableGrain, IDurableStackGrain
    {
        public async Task Push(string value)
        {
            state.Push(value);
            await WriteStateAsync();
        }

        public async Task<string> Pop()
        {
            var v = state.Pop();
            await WriteStateAsync();

            return v;
        }

        public async Task<TryValue<string>> TryPop()
        {
            var result = state.TryPop(out var item);
            await WriteStateAsync();

            return new(result, item);
        }

        public Task<string> Peek() => Task.FromResult(state.Peek());

        public async Task<TryValue<string>> TryPeek()
        {
            var result = state.TryPeek(out var item);
            await WriteStateAsync();

            return new(result, item);
        }

        public Task<int> GetCount() => Task.FromResult(state.Count);

        public async Task Clear()
        {
            state.Clear();
            await WriteStateAsync();
        }

        public Task<List<string>> GetAll() => Task.FromResult(state.ToList());
    }

    private IDurableStackGrain GetGrain(string key) => fixture.Cluster.Client.GetGrain<IDurableStackGrain>(key);
    private static ValueTask DeactivateGrain(IDurableStackGrain grain) => grain.Cast<IGrainManagementExtension>().DeactivateOnIdle();

    [Fact]
    public async Task EmptyOperations()
    {
        var grain = GetGrain("empty");

        Assert.Equal(0, await grain.GetCount());

        await Assert.ThrowsAsync<InvalidOperationException>(grain.Peek);
        await Assert.ThrowsAsync<InvalidOperationException>(grain.Pop);
    }

    [Fact]
    public async Task BasicOperations()
    {
        var grain = GetGrain("basic");

        await grain.Push("one");
        await grain.Push("two");
        await grain.Push("three");

        Assert.Equal(3, await grain.GetCount());

        var peeked = await grain.Peek();
        Assert.Equal("three", peeked);

        var tryPeek = await grain.TryPeek();
        Assert.True(tryPeek.Result);
        Assert.Equal("three", tryPeek.Item);

        var popped1 = await grain.Pop();
        Assert.Equal("three", popped1);
        Assert.Equal(2, await grain.GetCount());

        var tryPop = await grain.TryPop();
        Assert.True(tryPop.Result);
        Assert.Equal("two", tryPop.Item);
        Assert.Equal(1, await grain.GetCount());

        var popped3 = await grain.Pop();
        Assert.Equal("one", popped3);
        Assert.Equal(0, await grain.GetCount());

        var emptyTryPeek = await grain.TryPeek();
        Assert.False(emptyTryPeek.Result);
        Assert.Null(emptyTryPeek.Item);

        var emptyTryPop = await grain.TryPop();
        Assert.False(emptyTryPop.Result);
        Assert.Null(emptyTryPop.Item);
    }

    [Fact]
    public async Task Persistence()
    {
        var grain = GetGrain("persist");

        await grain.Push("one");
        await grain.Push("two");
        await grain.Push("three");

        await DeactivateGrain(grain);

        var count = await grain.GetCount();
        Assert.Equal(3, count);

        var peeked = await grain.Peek();
        Assert.Equal("three", peeked);

        var popped = await grain.Pop();
        Assert.Equal("three", popped);
        Assert.Equal(2, await grain.GetCount());
    }

    [Fact]
    public async Task Clear()
    {
        var grain = GetGrain("clear");

        await grain.Push("one");
        await grain.Push("two");
        await grain.Push("three");

        Assert.Equal(3, await grain.GetCount());

        await grain.Clear();
        Assert.Equal(0, await grain.GetCount());
    }

    [Fact]
    public async Task Enumeration()
    {
        var grain = GetGrain("enum");

        var expected = new List<string> { "one", "two", "three" };

        foreach (var v in expected)
        {
            await grain.Push(v);
        }

        var actual = await grain.GetAll();

        var expectedOrder = expected.AsEnumerable().Reverse().ToList();
        Assert.Equal(expectedOrder, actual);
    }

    [Fact]
    public async Task Restore()
    {
        var grain = GetGrain("restore");

        const int NumItems = 100;

        // We push a large number of items to ensure the state machine's logic
        // will trigger a snapshot operation upon deactivation.

        for (int i = 1; i <= NumItems; i++)
        {
            await grain.Push(i.ToString()); // Stack order: bottom=1, top=100
        }

        await DeactivateGrain(grain); // To trigger a restore from the snapshot

        // Verify the stack's LIFO integrity is preserved after restoration.

        for (int expected = NumItems; expected >= 1; expected--)
        {
            var actual = await grain.Pop();
            Assert.Equal(expected.ToString(), actual); // Should pop 100,99,...,1
        }

        Assert.Equal(0, await grain.GetCount());
    }
}