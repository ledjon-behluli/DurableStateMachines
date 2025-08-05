global using Orleans.Journaling;
global using Orleans.Core.Internal;
global using Ledjon.DurableStateMachines;

using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.DependencyInjection;
using Orleans.TestingHost;
using Microsoft.Extensions.Time.Testing;

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
            siloBuilder.AddStateMachineStorage();
            siloBuilder.Services.AddSingleton<TimeProvider>(TimeProvider);
            siloBuilder.Services.AddSingleton<IStateMachineStorageProvider>(_ => new VolatileStateMachineStorageProvider());
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