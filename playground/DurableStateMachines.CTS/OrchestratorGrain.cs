using Ledjon.DurableStateMachines;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans.Concurrency;
using Orleans.Journaling;
using System.Runtime.CompilerServices;

public interface IWorkerGrain : IGrainWithGuidKey
{
    IAsyncEnumerable<int> GetWorkItems(CancellationToken cancellationToken);
}

public interface IOrchestratorGrain : IGrainWithGuidKey
{
    Task StartProcessingWork();
    [AlwaysInterleave] Task SoftCancelWork();
    [AlwaysInterleave] Task CommitCancelWork();
}

public class OrchestratorGrain(
    ILogger<OrchestratorGrain> logger,
    [FromKeyedServices("workCancelStatus")] IDurableCancellationTokenSource cancelStatus)
        : DurableGrain, IOrchestratorGrain
{
    public async Task StartProcessingWork()
    {
        logger.LogInformation("Starting work and streaming from worker...");

        var worker = GrainFactory.GetGrain<IWorkerGrain>(Guid.NewGuid());
        var token = cancelStatus.Token;

        try
        {
            await foreach (var item in worker.GetWorkItems(token))
            {
                logger.LogInformation("Received '{Item}' from worker.", item);
            }

            logger.LogInformation("Work completed successfully.");
        }
        catch (OperationCanceledException)
        {
            logger.LogWarning("Work stream was canceled as requested.");
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Work stream failed with an exception.");
        }
    }

    public Task SoftCancelWork()
    {
        logger.LogWarning("Soft cancellation requested. Worker will continue to send items.");
        cancelStatus.Cancel();

        return Task.CompletedTask;
    }

    public async Task CommitCancelWork()
    {
        logger.LogWarning("Cancellation commit requested. Worker will stop sending items now.");
        cancelStatus.Cancel();
        await WriteStateAsync();
    }
}

public class WorkerGrain(ILogger<WorkerGrain> logger) : Grain, IWorkerGrain
{
    public async IAsyncEnumerable<int> GetWorkItems([EnumeratorCancellation] CancellationToken cancellationToken)
    {
        logger.LogInformation("I have started and will produce work until canceled.");

        for (var i = 1; i <= 100; i++)
        {
            try
            {
                cancellationToken.ThrowIfCancellationRequested();
                await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken);
            }
            catch (OperationCanceledException)
            {
                logger.LogWarning("Cancellation was requeted. I will stop producing work now.");
                break;
            }

            yield return i;
        }

        logger.LogInformation("I am exiting now.");
    }
}