using System.Buffers;
using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;

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

[DebuggerDisplay("RecordExists: {RecordExists}, Value: {Value}")]
internal sealed class DurableObject<T> : IDurableObject<T>, IDurableStateMachine where T : class, new()
{
    private const byte VersionByte = 0;

    private readonly IFieldCodec<T> _codec;
    private readonly SerializerSessionPool _sessionPool;

    private T? _value;
    private bool _exists;

    public DurableObject(
        [ServiceKey] string key, IStateMachineManager manager,
        IFieldCodec<T> codec, SerializerSessionPool sessionPool)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);

        _codec = codec;
        _sessionPool = sessionPool;

        manager.RegisterStateMachine(key, this);
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

    void IDurableStateMachine.OnWriteCompleted() => _exists = true;

    void IDurableStateMachine.Reset(IStateMachineLogWriter storage)
    {
        _value = null;
        _exists = false;
    }

    void IDurableStateMachine.Apply(ReadOnlySequence<byte> logEntry)
    {
        using var session = _sessionPool.GetSession();

        var reader = Reader.Create(logEntry, session);

        var version = reader.ReadByte();
        if (version != VersionByte)
        {
            throw new NotSupportedException($"This instance of {nameof(DurableObject<T>)} supports version {(uint)VersionByte} and not version {(uint)version}.");
        }

        var field = reader.ReadFieldHeader();

        _value = _codec.ReadValue(ref reader, field);
        _exists = true;
    }

    void IDurableStateMachine.AppendEntries(StateMachineStorageWriter logWriter)
    {
        // We always write the state when AppendEntries is called, because the state
        // object is mutated directly, and we can't easily track if it's dirty or not.
        // The whole goal of this component is the convenient direct mutation.

        WriteState(logWriter);
    }

    void IDurableStateMachine.AppendSnapshot(StateMachineStorageWriter snapshotWriter) => WriteState(snapshotWriter);

    private void WriteState(StateMachineStorageWriter writer)
    {
        writer.AppendEntry(static (self, bufferWriter) =>
        {
            using var session = self._sessionPool.GetSession();

            var writer = Writer.Create(bufferWriter, session);

            writer.WriteByte(VersionByte);

            self._codec.WriteField(ref writer, 0, typeof(T), self.Value);

            writer.Commit();
        }, this);
    }

    public IDurableStateMachine DeepCopy() => throw new NotImplementedException();
}