global using Orleans.Journaling;
global using Orleans.Serialization.Codecs;
global using Orleans.Serialization.Buffers;
global using Orleans.Serialization.Session;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Ledjon.DurableStateMachines;

/// <summary>
/// Extensions methods for durable state machines.
/// </summary>
public static class HostingExtensions
{
    /// <summary>
    /// Registers additional state machine implementations that support Orleans journaling,
    /// enabling durable behavior for common data structures.
    /// </summary>
    public static IServiceCollection AddDurableStateMachines(this IServiceCollection services)
    {
        services.TryAddKeyedScoped(typeof(IDurableStack<>), KeyedService.AnyKey, typeof(DurableStack<>));
        services.TryAddKeyedScoped(typeof(IDurablePriorityQueue<,>), KeyedService.AnyKey, typeof(DurablePriorityQueue<,>));
        services.TryAddKeyedScoped(typeof(IDurableListLookup<,>), KeyedService.AnyKey, typeof(DurableListLookup<,>));
        services.TryAddKeyedScoped(typeof(IDurableSetLookup<,>), KeyedService.AnyKey, typeof(DurableSetLookup<,>));
        services.TryAddKeyedScoped(typeof(IDurableTree<>), KeyedService.AnyKey, typeof(DurableTree<>));
        services.TryAddKeyedScoped(typeof(IDurableGraph<,>), KeyedService.AnyKey, typeof(DurableGraph<,>));
        services.TryAddKeyedScoped(typeof(IDurableCancellationTokenSource), KeyedService.AnyKey, typeof(DurableCancellationTokenSource));
        services.TryAddKeyedScoped(typeof(IDurableObject<>), KeyedService.AnyKey, typeof(DurableObject<>));
        services.TryAddKeyedScoped(typeof(IDurableOrderedSet<>), KeyedService.AnyKey, typeof(DurableOrderedSet<>));
        services.TryAddKeyedScoped(typeof(IDurableOrderedSetLookup<,>), KeyedService.AnyKey, typeof(DurableOrderedSetLookup<,>));

        return services;
    }
}