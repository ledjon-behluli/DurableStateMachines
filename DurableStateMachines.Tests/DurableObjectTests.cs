using Microsoft.Extensions.DependencyInjection;
using static DurableStateMachines.Tests.DurableObjectTests;

namespace DurableStateMachines.Tests;

[Collection(GlobalFixture.Name)]
public class DurableObjectTests(TestFixture fixture)
{
    [GenerateSerializer]
    public class ClassState
    {
        [Id(0)] public int Counter { get; set; }
        [Id(1)] public ClassNestedState Nested { get; set; } = new();
        [Id(2)] public List<string> Items { get; set; } = [];
        [Id(3)] public Dictionary<string, int> Mappings { get; set; } = [];
    }

    [GenerateSerializer]
    public class ClassNestedState
    {
        [Id(0)] public string? Name { get; set; }
    }

    public interface IDurableObjectGrain : IGrainWithStringKey
    {
        Task<bool> RecordExists();
        Task<ClassState> GetState();
        Task SetNullState();
        Task ReplaceState(ClassState newState);
        Task MutateState(string nestedName, string listItem, string mapKey, int mapValue);
    }

    public class DurableObjectGrain(
        [FromKeyedServices("state")] IDurableObject<ClassState> state)
            : DurableGrain, IDurableObjectGrain
    {
        public Task<bool> RecordExists() => Task.FromResult(state.RecordExists);
        public Task<ClassState> GetState() => Task.FromResult(state.Value);
        public Task SetNullState()
        {
            state.Value = null!;
            return Task.CompletedTask;
        }

        public async Task MutateState(string nestedName, string listItem, string mapKey, int mapValue)
        {
            state.Value.Counter++;
            state.Value.Nested.Name = nestedName;
            state.Value.Items.Add(listItem);
            state.Value.Mappings[mapKey] = mapValue;

            await WriteStateAsync();
        }

        public async Task ReplaceState(ClassState newState)
        {
            state.Value = newState;
            await WriteStateAsync();
        }
    }

    [GenerateSerializer]
    public record RecordState
    {
        [Id(0)] public int Counter { get; init; }
        [Id(1)] public List<string> Items { get; init; } = [];
    }

    public interface IDurableObjectRecordGrain : IGrainWithStringKey
    {
        Task<bool> RecordExists();
        Task<RecordState> GetState();
        Task ReplaceState(RecordState newState);
    }

    public class DurableObjectRecordGrain(
        [FromKeyedServices("state")] IDurableObject<RecordState> state)
            : DurableGrain, IDurableObjectRecordGrain
    {
        public Task<bool> RecordExists() => Task.FromResult(state.RecordExists);
        public Task<RecordState> GetState() => Task.FromResult(state.Value);

        public async Task ReplaceState(RecordState newState)
        {
            state.Value = newState;
            await WriteStateAsync();
        }
    }

    private IDurableObjectGrain GetGrain(string key) => fixture.Cluster.Client.GetGrain<IDurableObjectGrain>(key);
    private IDurableObjectRecordGrain GetRecordGrain(string key) => fixture.Cluster.Client.GetGrain<IDurableObjectRecordGrain>(key);

    private static ValueTask DeactivateGrain(IGrain grain) => grain.Cast<IGrainManagementExtension>().DeactivateOnIdle();

    [Fact]
    public async Task SettingNullState_ShouldThrow()
    {
        await Assert.ThrowsAsync<ArgumentNullException>(() => GetGrain("null").SetNullState());
    }

    [Fact]
    public async Task InitialState_ShouldNotExist_AndBeDefault()
    {
        var grain = GetGrain("initial");

        var exists = await grain.RecordExists();
        Assert.False(exists);

        var state = await grain.GetState();

        Assert.NotNull(state);
        Assert.Equal(0, state.Counter);
        Assert.NotNull(state.Nested);
        Assert.Null(state.Nested.Name);
        Assert.NotNull(state.Items);
        Assert.Empty(state.Items);
        Assert.NotNull(state.Mappings);
        Assert.Empty(state.Mappings);
    }

    [Fact]
    public async Task DirectMutation_ShouldPersistChanges()
    {
        var grain = GetGrain("mutate");

        await grain.MutateState("nested-name", "list-item-1", "map-key-1", 100);
        await grain.MutateState("new-nested-name", "list-item-2", "map-key-2", 200);

        var exists = await grain.RecordExists();
        var state = await grain.GetState();

        Assert.True(exists);
        Assert.Equal(2, state.Counter);
        Assert.Equal("new-nested-name", state.Nested.Name);
        Assert.Equal(2, state.Items.Count);
        Assert.Contains("list-item-1", state.Items);
        Assert.Contains("list-item-2", state.Items);
        Assert.Equal(2, state.Mappings.Count);
        Assert.Equal(100, state.Mappings["map-key-1"]);
        Assert.Equal(200, state.Mappings["map-key-2"]);
    }

    [Fact]
    public async Task ObjectReplacement_ShouldPersistChanges()
    {
        var grain = GetGrain("replace");
        var newState = new ClassState
        {
            Counter = 99,
            Nested = new ClassNestedState { Name = "replaced-nested" },
            Items = ["a", "b"],
            Mappings = new Dictionary<string, int> { { "k", 1 } }
        };

        await grain.ReplaceState(newState);

        var exists = await grain.RecordExists();
        var state = await grain.GetState();

        Assert.True(exists);
        Assert.Equal(99, state.Counter);
        Assert.Equal("replaced-nested", state.Nested.Name);
        Assert.Equal(["a", "b"], state.Items);
        Assert.Single(state.Mappings);
        Assert.Equal(1, state.Mappings["k"]);
    }

    [Fact]
    public async Task Persistence_ShouldRestoreState_AfterDeactivation()
    {
        var grain = GetGrain("persistence");

        var existsBefore = await grain.RecordExists();
        Assert.False(existsBefore);

        await grain.MutateState("final-name", "final-item", "final-key", 999);

        var stateBefore = await grain.GetState();

        await DeactivateGrain(grain);

        var existsAfter = await grain.RecordExists();
        var stateAfter = await grain.GetState();

        Assert.True(existsAfter);
        Assert.Equal(1, stateAfter.Counter);
        Assert.Equal(stateBefore.Nested.Name, stateAfter.Nested.Name);
        Assert.Equal(stateBefore.Items, stateAfter.Items);
        Assert.Equal(stateBefore.Mappings, stateAfter.Mappings);
    }

    [Fact]
    public async Task RecordType_ShouldPersist_AfterDeactivation()
    {
        var grain = GetRecordGrain("record-persistence");

        var existsBefore = await grain.RecordExists();
        Assert.False(existsBefore);

        var expectedState = new RecordState
        {
            Counter = 42,
            Items = ["item1", "item2"]
        };

        await grain.ReplaceState(expectedState);

        await DeactivateGrain(grain);

        var actualState = await grain.GetState();

        Assert.Equal(expectedState.Counter, actualState.Counter);
        Assert.Equal(expectedState.Items, actualState.Items);
    }
}