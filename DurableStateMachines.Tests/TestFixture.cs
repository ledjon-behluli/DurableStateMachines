global using Ledjon.DurableStateMachines;
global using Orleans.Core.Internal;
global using Orleans.Journaling;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Time.Testing;
using Orleans.Journaling.Json;
using Orleans.TestingHost;
using System.Diagnostics.CodeAnalysis;

namespace DurableStateMachines.Tests;

[GenerateSerializer]
public readonly record struct TryValue<T>(bool Result, [NotNullWhen(true)] T? Item);

[CollectionDefinition(Name)]
public class GlobalFixture : ICollectionFixture<TestFixture>
{
    public const string Name = "Durable State Machines";
}

public class TestFixture : IAsyncLifetime
{
    public FakeTimeProvider TimeProvider { get; } = new();
    public InProcessTestCluster Cluster { get; }

    public TestFixture()
    {
        var builder = new InProcessTestClusterBuilder(1);
        
        builder.ConfigureSilo((options, siloBuilder) =>
        {
            siloBuilder.AddJournalStorage();
            siloBuilder.Services.Configure<JournaledStateManagerOptions>(options => options.JournalFormatKey = "orleans-binary");
            siloBuilder.Services.AddSingleton<TimeProvider>(TimeProvider);
            siloBuilder.Services.AddSingleton<IJournalStorageProvider, VolatileJournalStorageProvider>();
            siloBuilder.Services.AddDurableStateMachines();
        });

        Cluster = builder.Build();
    }

    public virtual async Task InitializeAsync()
    {
        await Cluster.DeployAsync();
    }

    public virtual async Task DisposeAsync()
    {
        await Cluster.DisposeAsync();
    }
}