using Microsoft.Extensions.DependencyInjection;

namespace DurableStateMachines.Tests;

[Collection(GlobalFixture.Name)]
public class DurableCancellationTokenTests(TestFixture fixture)
{
    public interface IDurableCancellationTokenGrain : IGrainWithStringKey
    {
        Task Cancel(bool persist = true);
        Task CancelAfter(TimeSpan delay);
        Task ThrowIfCancellationRequested();
        Task<bool> IsCancellationPending();
        Task<bool> IsCancellationRequested();
    }

    public class DurableCancellationTokenGrain([FromKeyedServices("cts")]
        IDurableCancellationTokenSource state) : DurableGrain, IDurableCancellationTokenGrain
    {
        public async Task Cancel(bool persist)
        {
            state.Cancel();
            if (persist)
            {
                await WriteStateAsync();
            }
        }

        public async Task CancelAfter(TimeSpan delay)
        {
            state.CancelAfter(delay);
            await WriteStateAsync();
        }

        public Task ThrowIfCancellationRequested()
        {
            state.Token.ThrowIfCancellationRequested();
            return Task.CompletedTask;
        }

        public Task<bool> IsCancellationPending() => Task.FromResult(state.IsCancellationPending);
        public Task<bool> IsCancellationRequested() => Task.FromResult(state.Token.IsCancellationRequested);
    }

    private IDurableCancellationTokenGrain GetGrain(string key) => fixture.Cluster.Client.GetGrain<IDurableCancellationTokenGrain>(key);
    private static ValueTask DeactivateGrain(IDurableCancellationTokenGrain grain) => grain.Cast<IGrainManagementExtension>().DeactivateOnIdle();

    [Fact]
    public async Task Cancel()
    {
        var grain = GetGrain("cancel");

        Assert.False(await grain.IsCancellationPending());
        Assert.False(await grain.IsCancellationRequested());
        await grain.ThrowIfCancellationRequested(); // Should not throw

        await grain.Cancel();

        Assert.True(await grain.IsCancellationPending());
        Assert.True(await grain.IsCancellationRequested());
        await Assert.ThrowsAsync<OperationCanceledException>(grain.ThrowIfCancellationRequested);

        await grain.Cancel(); // Cancel should be idempotent
        Assert.True(await grain.IsCancellationPending());
        Assert.True(await grain.IsCancellationRequested());
    }

    [Fact]
    public async Task SoftCancel()
    {
        var grain = GetGrain("soft_cancel");

        Assert.False(await grain.IsCancellationPending());

        await grain.Cancel(persist: false);

        Assert.True(await grain.IsCancellationPending());
        Assert.False(await grain.IsCancellationRequested());
    }

    [Fact]
    public async Task Cancel_Persists_AfterReactivation()
    {
        var grain = GetGrain("cancel_persists");

        await grain.Cancel();

        Assert.True(await grain.IsCancellationPending());

        await DeactivateGrain(grain);

        Assert.True(await grain.IsCancellationPending());
        await Assert.ThrowsAsync<OperationCanceledException>(grain.ThrowIfCancellationRequested);
    }

    [Fact]
    public async Task CancelAfter()
    {
        var grain = GetGrain("cancel_after");
        var delay = TimeSpan.FromSeconds(1);

        await grain.CancelAfter(delay);

        Assert.False(await grain.IsCancellationPending());

        fixture.TimeProvider.Advance(delay + TimeSpan.FromMilliseconds(250));

        Assert.True(await grain.IsCancellationPending());
        await Assert.ThrowsAsync<OperationCanceledException>(grain.ThrowIfCancellationRequested);
    }

    [Fact]
    public async Task CancelAfter_FiresLater_AfterReactivation()
    {
        var grain = GetGrain("cancel_after_deactivated_expired");
        var delay = TimeSpan.FromSeconds(2);

        await grain.CancelAfter(delay);
        await DeactivateGrain(grain);

        fixture.TimeProvider.Advance(delay + TimeSpan.FromMilliseconds(250));

        Assert.True(await grain.IsCancellationPending());
        await Assert.ThrowsAsync<OperationCanceledException>(grain.ThrowIfCancellationRequested);
    }

    [Fact]
    public async Task CancelAfter_RehydratesTimerAndFiresLater_AfterReactivation()
    {
        var grain = GetGrain("cancel_after_rehydrates_timer");
        var fullDelay = TimeSpan.FromSeconds(4);
        var firstWait = TimeSpan.FromSeconds(1);
        var remainingWait = fullDelay - firstWait;

        await grain.CancelAfter(fullDelay);
        await DeactivateGrain(grain);

        fixture.TimeProvider.Advance(firstWait);

        Assert.False(await grain.IsCancellationPending());

        fixture.TimeProvider.Advance(remainingWait + TimeSpan.FromMilliseconds(500));

        Assert.True(await grain.IsCancellationPending());
        await Assert.ThrowsAsync<OperationCanceledException>(grain.ThrowIfCancellationRequested);
    }

    [Fact]
    public async Task ImmediateCancel_Overrides_ScheduledCancel()
    {
        var grain = GetGrain("cancel_overrides_scheduled");

        await grain.CancelAfter(TimeSpan.FromSeconds(30));

        Assert.False(await grain.IsCancellationPending());

        await grain.Cancel();

        Assert.True(await grain.IsCancellationPending());
        await Assert.ThrowsAsync<OperationCanceledException>(grain.ThrowIfCancellationRequested);
    }

    [Fact]
    public async Task ShorterCancelAfter_Overrides_LongerScheduledCancel()
    {
        var grain = GetGrain("shorter_overrides_longer");

        var shorterDelay = TimeSpan.FromSeconds(1);
        var longerDelay = TimeSpan.FromSeconds(10);

        await grain.CancelAfter(longerDelay);
        await grain.CancelAfter(shorterDelay);

        fixture.TimeProvider.Advance(shorterDelay + TimeSpan.FromMilliseconds(250));

        Assert.True(await grain.IsCancellationPending());
        await Assert.ThrowsAsync<OperationCanceledException>(grain.ThrowIfCancellationRequested);
    }
}