using Ledjon.DurableStateMachines;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans.Journaling;

public interface IJobGrain : IGrainWithGuidKey
{
    Task Start(JobParameters parameters);
    Task Cancel();
    Task<JobStatus> GetStatus();
}

public class JobGrain(
    ILogger<JobGrain> logger,
    [FromKeyedServices("currentStep")] IDurableValue<ProcessingStep> currentStep,
    [FromKeyedServices("cancelStatus")] IDurableCancellationTokenSource cancelStatus,
    [FromKeyedServices("finalResult")] IDurableTaskCompletionSourceFixed<string> finalResult)
        : DurableGrain, IJobGrain
{
    public async Task Start(JobParameters parameters)
    {
        if (cancelStatus.IsCancellationPending)
        {
            logger.LogInformation("Job was already canceled at step '{CurrentStep}'.", currentStep.Value);
            return;
        }

        if (finalResult.State.Status is DurableTaskCompletionSourceStatusFixed.Pending &&
            currentStep.Value > ProcessingStep.NotStarted)
        {
            logger.LogInformation("Resuming job at step '{CurrentStep}'.", currentStep.Value);
            Run(null).Ignore();

            return;
        }

        logger.LogInformation("Starting new job {JobId}.", parameters.JobId);

        currentStep.Value = ProcessingStep.FetchingData;
        cancelStatus.CancelAfter(parameters.OverallTimeout);

        await WriteStateAsync();

        Run(parameters).Ignore();
    }

    public async Task Cancel()
    {
        logger.LogWarning("User-initiated cancellation received.");

        cancelStatus.Cancel();

        await WriteStateAsync();
    }

    public Task<JobStatus> GetStatus() => Task.FromResult(new JobStatus(currentStep.Value, finalResult.State));

    private async Task Run(JobParameters? parameters)
    {
        var token = cancelStatus.Token;

        try
        {
            while (currentStep.Value != ProcessingStep.Finished && !token.IsCancellationRequested)
            {
                switch (currentStep.Value)
                {
                    case ProcessingStep.FetchingData:
                        {
                            logger.LogInformation("Fetching data...");
                            await Task.Delay(TimeSpan.FromSeconds(3), token);

                            currentStep.Value = ProcessingStep.ProcessingData;
                            await WriteStateAsync();
                        }
                        break;

                    case ProcessingStep.ProcessingData:
                        {
                            logger.LogInformation("Processing data...");
                            await Task.Delay(TimeSpan.FromSeconds(4), token);

                            if (parameters?.ShouldFail == true)
                            {
                                throw new InvalidOperationException("A simulated failure occurred during processing!");
                            }

                            currentStep.Value = ProcessingStep.SavingResults;
                            await WriteStateAsync();
                        }
                        break;

                    case ProcessingStep.SavingResults:
                        {
                            logger.LogInformation("Saving results...");
                            await Task.Delay(TimeSpan.FromSeconds(2), token);

                            currentStep.Value = ProcessingStep.Finished;
                            await WriteStateAsync();
                        }
                        break;
                }
            }

            token.ThrowIfCancellationRequested();

            logger.LogInformation("Job completed successfully.");

            finalResult.TrySetResult($"Job {parameters?.JobId ?? "Resumed Job"} completed successfully.");
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
            logger.LogWarning("Job was canceled.");
            finalResult.TrySetCanceled();
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Job failed with an exception.");
            finalResult.TrySetException(ex);
        }
        finally
        {
            await WriteStateAsync();
        }
    }
}

[GenerateSerializer]
public record JobParameters(string JobId, TimeSpan OverallTimeout, bool ShouldFail);

[GenerateSerializer]
public record JobStatus(ProcessingStep CurrentStep, DurableTaskCompletionSourceStateFixed<string> FinalState);

[GenerateSerializer]
public enum ProcessingStep { NotStarted, FetchingData, ProcessingData, SavingResults, Finished }