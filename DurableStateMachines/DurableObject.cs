using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using System.Diagnostics;

namespace Ledjon.DurableStateMachines;

/// <summary>
/// <para>Represents a durable reference to an object, allowing for direct mutation.</para>
/// <para>The benefit of this component is that the object <typeparamref name="T"/> can be mutated directly.</para>
/// </summary>
/// <typeparam name="T">The type of the state object. Must be a class with a parameterless constructor.</typeparam>
/// <remarks>
/// You can think of this component like a <see cref="IDurableValue{T}"/>, but built with reference types in mind,
/// specifically the ability to do direct mutations. And while <see cref="DurableState{T}"/> achives that,
/// this component simplifies the API surface to work with.
/// </remarks>
public interface IDurableObject<T> where T : class, new()
{
    /// <summary>
    /// <para>Gets or sets the state object. This property will never return <c>null</c>.</para>
    /// <para>When getting, if the state has not been set or loaded, a new instance is created.</para>
    /// <para>When setting, the state is unconditionally marked for persistence on the next WriteStateAsync call.</para>
    /// </summary>
    /// <remarks>Null values are not allowed.</remarks>
    /// <exception cref="ArgumentNullException"/>
    T Value { get; set; }

    /// <summary>
    /// Gets a value indicating whether the state record was loaded from storage, or has been successfully written at least once.
    /// </summary>
    bool RecordExists { get; }
}

/// <summary>
/// Receives decoded durable object commands from a codec implementation.
/// </summary>
public interface IDurableObjectCommandHandler<T>
{
    /// <summary>Applies a state command.</summary>
    void ApplyState(T value);

    /// <summary>Resets the receiver before applying replacement entries.</summary>
    void Reset();
}

/// <summary>
/// Serializes one durable object command and applies one decoded command.
/// </summary>
public interface IDurableObjectCommandCodec<T>
{
    /// <summary>Writes a state command.</summary>
    void WriteState(T value, JournalStreamWriter writer);

    /// <summary>Reads one encoded command and applies it to <paramref name="handler"/>.</summary>
    void Apply(JournalBufferReader input, IDurableObjectCommandHandler<T> handler);
}

internal sealed class DurableObjectCommandBinaryCodec<T>(
    IFieldCodec<T> codec, SerializerSessionPool sessionPool) : IDurableObjectCommandCodec<T>
{
    private const byte VersionByte = 0;

    public void WriteState(T value, JournalStreamWriter writer)
    {
        using var entry = writer.BeginEntry();
        using var session = sessionPool.GetSession();

        var payloadWriter = Writer.Create(entry.Writer, session);

        payloadWriter.WriteByte(VersionByte);
        codec.WriteField(ref payloadWriter, 0, typeof(T), value);

        payloadWriter.Commit();
        entry.Commit();
    }

    public void Apply(JournalBufferReader input, IDurableObjectCommandHandler<T> handler)
    {
        ArgumentNullException.ThrowIfNull(handler);

        using var slice = input.Peek(input.Length);
        using var session = sessionPool.GetSession();

        var reader = Reader.Create(slice, session);
        var version = reader.ReadByte();

        if (version != VersionByte)
        {
            throw new NotSupportedException($"This command codec supports version {(uint)VersionByte} and not version {(uint)version}.");
        }

        var field = reader.ReadFieldHeader();
        handler.ApplyState(codec.ReadValue(ref reader, field));

        Helpers.ThrowIfTrailingData(ref reader);
    }
}

[DebuggerDisplay("RecordExists: {RecordExists}, Value: {Value}")]
internal sealed class DurableObject<T> :
    IDurableObject<T>,
    IDurableObjectCommandHandler<T>,
    IJournaledState where T : class, new()
{
    private T? _value;
    private bool _exists;
    private readonly IDurableObjectCommandCodec<T> _codec;

    public DurableObject(
        [ServiceKey] string key, IJournaledStateManager manager,
        IOptions<JournaledStateManagerOptions> options, IServiceProvider serviceProvider)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        _codec = Helpers.GetCodec<IDurableObjectCommandCodec<T>>(serviceProvider, options);
        manager.RegisterState(key, this);
    }

    public T Value
    {
        get => _value ??= new();
        set
        {
            ArgumentNullException.ThrowIfNull(value);
            _value = value;
        }
    }

    public bool RecordExists => _exists;

    #region IJournalState

    IJournaledState IJournaledState.DeepCopy() => throw new NotImplementedException();

    void IJournaledState.ReplayEntry(JournalEntry entry, JournalReplayContext context) =>
        context.GetRequiredCommandCodec(entry.FormatKey, _codec).Apply(entry.Reader, this);

    void IJournaledState.AppendEntries(JournalStreamWriter writer)
    {
        // We always write the state when AppendEntries is called, because the state
        // object is mutated directly, and we can't easily track if it's dirty or not.
        // The whole goal of this component is the convenient direct mutation.
        _codec.WriteState(Value, writer);
    }

    void IJournaledState.AppendSnapshot(JournalStreamWriter snapshotWriter) => _codec.WriteState(Value, snapshotWriter);

    void IJournaledState.Reset(JournalStreamWriter writer)
    {
        _value = null;
        _exists = false;
    }

    void IJournaledState.OnWriteCompleted() => _exists = true;

    #endregion

    #region IDurableObjectCommandHandler

    void IDurableObjectCommandHandler<T>.ApplyState(T value)
    {
        _value = value;
        _exists = true;
    }

    void IDurableObjectCommandHandler<T>.Reset()
    {
        _value = null;
        _exists = false;
    }

    #endregion
}