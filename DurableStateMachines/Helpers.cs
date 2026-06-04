using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using System.Runtime.CompilerServices;

namespace Ledjon.DurableStateMachines;

internal static class Helpers
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void ThrowUnsupportedCommand<T>(T command) where T : Enum =>
        throw new NotSupportedException($"Command type {command} is not supported");

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void ThrowIfTrailingData<TInput>(ref Reader<TInput> reader)
    {
        if (reader.Position != reader.Length)
        {
            throw new InvalidOperationException("Unexpected trailing data after binary journal command.");
        }
    }

    public static TCodec GetCodec<TCodec>(
        IServiceProvider serviceProvider, IOptions<JournaledStateManagerOptions> options)
            where TCodec : notnull
    {
        var formatKey = options.Value.JournalFormatKey;

        ArgumentNullException.ThrowIfNull(formatKey, nameof(formatKey));

        if (serviceProvider.GetKeyedService<TCodec>(formatKey) is not { } codec)
        {
            throw new InvalidOperationException(
                $"'Ledjon.DurableStateMachines' does not include a built-in codec for the '{formatKey}' journal format. " +
                $"Orleans requires an implementation of '{typeof(TCodec).Name}', but none was registered under the '{formatKey}' key.\n\n" +
                $"To fix this, you have two options:\n" +
                $"1. Configure Orleans to use the binary format globally (if suitable for your app):\n" +
                $"   siloBuilder.AddJournalStorage(options => options.JournalFormatKey = \"orleans-binary\");\n" +
                $"2. Implement '{typeof(TCodec).Name}' yourself and register it in your DI container as a KeyedSingleton under the \"{formatKey}\" key.");
        }

        return codec;
    }
}