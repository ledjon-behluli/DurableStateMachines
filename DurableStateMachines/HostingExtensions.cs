global using Orleans.Journaling;
global using Orleans.Serialization.Codecs;
global using Orleans.Serialization.Buffers;
global using Orleans.Serialization.Session;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Ledjon.DurableStateMachines;

public static class HostingExtensions
{
    public static IServiceCollection AddDurableStateMachines(this IServiceCollection services)
    {
        services.TryAddKeyedScoped(typeof(IDurableStack<>), KeyedService.AnyKey, typeof(DurableStack<>));
        services.TryAddKeyedScoped(typeof(IDurablePriorityQueue<,>), KeyedService.AnyKey, typeof(DurablePriorityQueue<,>));
        services.TryAddKeyedScoped(typeof(IDurableListLookup<,>), KeyedService.AnyKey, typeof(DurableListLookup<,>));
        services.TryAddKeyedScoped(typeof(IDurableSetLookup<,>), KeyedService.AnyKey, typeof(DurableSetLookup<,>));
        services.TryAddKeyedScoped(typeof(IDurableTree<>), KeyedService.AnyKey, typeof(DurableTree<>));
        services.TryAddKeyedScoped(typeof(IDurableGraph<,>), KeyedService.AnyKey, typeof(DurableGraph<,>));

        return services;
    }
}