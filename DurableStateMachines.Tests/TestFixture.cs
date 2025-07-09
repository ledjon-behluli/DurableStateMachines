global using Orleans.Journaling;
global using Orleans.Core.Internal;
global using Ledjon.DurableStateMachines;

using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.DependencyInjection;
using Orleans.TestingHost;

namespace DurableStateMachines.Tests;

[GenerateSerializer]
public readonly record struct TryValue<T>(bool Result, [NotNullWhen(true)] T? Item);

public class TestFixture : IAsyncLifetime
{
    public readonly InProcessTestCluster Cluster;

    public TestFixture()
    {
        var builder = new InProcessTestClusterBuilder(1);
        
        builder.ConfigureSilo((options, siloBuilder) =>
        {
            siloBuilder.AddStateMachineStorage();
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