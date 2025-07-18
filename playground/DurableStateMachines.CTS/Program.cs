using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Ledjon.DurableStateMachines;
using Orleans.Journaling;
using Orleans.Core.Internal;

var host = Host.CreateDefaultBuilder(args)
    .ConfigureLogging(builder =>
    {
        builder.AddConsole().SetMinimumLevel(LogLevel.Error);
        builder.AddFilter(typeof(Program).FullName!, LogLevel.Information);
        builder.AddFilter(typeof(JobGrain).FullName!, LogLevel.Information);
        builder.AddFilter(typeof(WorkerGrain).FullName!, LogLevel.Information);
        builder.AddFilter(typeof(OrchestratorGrain).FullName!, LogLevel.Information);
    })
    .UseOrleans(builder =>
    {
        builder.UseLocalhostClustering();
        builder.AddStateMachineStorage();
        builder.Services.AddSingleton<IStateMachineStorageProvider>(_ => new VolatileStateMachineStorageProvider());
        builder.Services.AddDurableStateMachines();

        // NOTE: The 'IDurableTaskCompletionSource' found in Orleans has a bug where it can lead to
        // 'Insufficient data present in buffer' exception. This is a fixed version of it until
        // this PR is merged and a new version is released. See: https://github.com/dotnet/orleans/pull/9626
        builder.Services.TryAddKeyedScoped(typeof(IDurableTaskCompletionSourceFixed<>), KeyedService.AnyKey, typeof(DurableTaskCompletionSourceFixed<>));

    })
    .Build();

await host.StartAsync();

var logger = host.Services.GetRequiredService<ILogger<Program>>();
var grainFactory = host.Services.GetRequiredService<IGrainFactory>();

// NOTE: It may happen that you get an error 'Collection was modified; enumeration operation may not execute.'
// while running these scenarios, there is a bug in the journaling framework.
// See: https://github.com/dotnet/orleans/pull/9624

logger.LogInformation("\n--- SCENARIO 1: SUCCESSFUL JOB RUN ---\n");
await RunJob(new JobParameters("job1", TimeSpan.FromSeconds(20), ShouldFail: false));

logger.LogInformation("\n--- SCENARIO 2: USER-INITIATED CANCELLATION ---\n");
await RunJob(new JobParameters("job2", TimeSpan.FromSeconds(20), ShouldFail: false), cancelAfter: TimeSpan.FromSeconds(3));

logger.LogInformation("\n--- SCENARIO 3: AUTOMATIC TIMEOUT ---\n");
await RunJob(new JobParameters("job3", TimeSpan.FromSeconds(5), ShouldFail: false));

logger.LogInformation("\n--- SCENARIO 4: JOB FAILURE ---\n");
await RunJob(new JobParameters("job4", TimeSpan.FromSeconds(20), ShouldFail: true));

logger.LogInformation("\n--- SCENARIO 5: RESUMPTION OF A CANCELED JOB ---\n");
await RunResilientJob();

logger.LogInformation("\n--- SCENARIO 6: CHAINED CANCELLATION ---\n");
await RunChainedCancellation();

logger.LogInformation("\nAll scenarios complete. Press any key to exit.");

Console.ReadKey();

await host.StopAsync();

async Task RunJob(JobParameters parameters, TimeSpan? cancelAfter = null)
{
    var grain = grainFactory.GetGrain<IJobGrain>(Guid.NewGuid());

    logger.LogInformation("Starting job {JobId}...", parameters.JobId);

    await grain.Start(parameters);

    if (cancelAfter.HasValue)
    {
        await Task.Delay(cancelAfter.Value);

        logger.LogInformation("--> User requesting cancellation for job {JobId}...", parameters.JobId);

        await grain.Cancel();
    }

    while (true)
    {
        var status = await grain.GetStatus();

        logger.LogInformation("Polling... Job {JobId} is in step '{Step}' with status '{Status}'",
            parameters.JobId, status.CurrentStep, status.FinalState.Status);

        if (status.FinalState.Status != DurableTaskCompletionSourceStatusFixed.Pending)
        {
            logger.LogInformation("--> Job {JobId} finished with status: {Status}", parameters.JobId, status.FinalState.Status);

            if (status.FinalState.Status == DurableTaskCompletionSourceStatusFixed.Completed)
            {
                logger.LogInformation("--> Result: {Result}", status.FinalState.Status);
            }
            else if (status.FinalState.Status == DurableTaskCompletionSourceStatusFixed.Faulted)
            {
                logger.LogError("--> Exception: {ExceptionMessage}", status.FinalState.Exception?.Message);
            }
            break;
        }

        await Task.Delay(2000);
    }
}

async Task RunResilientJob()
{
    var grain = grainFactory.GetGrain<IJobGrain>(Guid.NewGuid());

    var parameters = new JobParameters("job5", TimeSpan.FromMinutes(1), ShouldFail: false);
    logger.LogInformation("Starting resilient job {JobId}...", parameters.JobId);
    await grain.Start(parameters);

    logger.LogInformation("--> Waiting 2 seconds for the job to start processing...");
    await Task.Delay(TimeSpan.FromSeconds(2));

    logger.LogInformation("--> User requesting cancellation for job {JobId}...", parameters.JobId);
    await grain.Cancel();

    logger.LogInformation("--> Cancellation is persisted. Forcing deactivation now...");
    await grain.Cast<IGrainManagementExtension>().DeactivateOnIdle();

    await Task.Delay(TimeSpan.FromSeconds(1)); // Give it a moment.
    logger.LogInformation("--> Grain should be deactivated. Waking it up to see if it cleans up correctly...");

    logger.LogInformation("Starting resilient job {JobId} again, after deactivation...", parameters.JobId);
    await grain.Start(parameters);

    var status = await grain.GetStatus();
    logger.LogInformation("Polling... Job {JobId} has final status '{Status}'", parameters.JobId, status.FinalState.Status);

    if (status.FinalState.Status == DurableTaskCompletionSourceStatusFixed.Canceled)
    {
        logger.LogInformation("--> SUCCESS: The reactivated grain correctly recognized its canceled state.");
    }
    else
    {
        logger.LogError("--> FAILURE: The reactivated grain did not correctly enter a Canceled state. Status is {Status}", status.FinalState.Status);
    }
}

async Task RunChainedCancellation()
{
    var grain = grainFactory.GetGrain<IOrchestratorGrain>(Guid.NewGuid());

    logger.LogInformation("--> Kicking off the orchestrator to start the worker stream.");
    grain.StartProcessingWork().Ignore();

    logger.LogInformation("--> Work is running. Waiting 4 seconds before canceling.");
    await Task.Delay(TimeSpan.FromSeconds(4));

    logger.LogInformation("--> Sending soft cancellation request to the orchestrator. Waiting 3 seconds after commiting.");
    await grain.SoftCancelWork();
    await Task.Delay(TimeSpan.FromSeconds(3));

    logger.LogInformation("--> Sending cancellation commit to the orchestrator.");
    await grain.CommitCancelWork();

    await Task.Delay(TimeSpan.FromSeconds(1)); // Some room to print the final log
}