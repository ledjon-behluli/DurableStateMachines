using System.Buffers;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Journaling;
using Orleans.Serialization;
using Orleans.Serialization.Buffers;
using Orleans.Serialization.Codecs;
using Orleans.Serialization.Session;

public interface IDurableTaskCompletionSourceFixed<T>
{
    Task<T> Task { get; }
    DurableTaskCompletionSourceStateFixed<T> State { get; }

    bool TrySetCanceled();
    bool TrySetException(Exception exception);
    bool TrySetResult(T value);
}

[DebuggerDisplay("Status = {Status}")]
internal sealed class DurableTaskCompletionSourceFixed<T> : IDurableTaskCompletionSourceFixed<T>, IDurableStateMachine
{
    private const byte SupportedVersion = 0;
    private readonly SerializerSessionPool _serializerSessionPool;
    private readonly IFieldCodec<T> _codec;
    private readonly IFieldCodec<Exception> _exceptionCodec;
    private readonly DeepCopier<T> _copier;
    private readonly DeepCopier<Exception> _exceptionCopier;

    private TaskCompletionSource<T> _completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private IStateMachineLogWriter? _storage;
    private DurableTaskCompletionSourceStatusFixed _status;
    private T? _value;
    private Exception? _exception;

    public DurableTaskCompletionSourceFixed(
        [ServiceKey] string key,
        IStateMachineManager manager,
        IFieldCodec<T> codec,
        DeepCopier<T> copier,
        IFieldCodec<Exception> exceptionCodec,
        DeepCopier<Exception> exceptionCopier,
        SerializerSessionPool serializerSessionPool)
    {
        ArgumentNullException.ThrowIfNullOrEmpty(key);
        _codec = codec;
        _copier = copier;
        _exceptionCodec = exceptionCodec;
        _exceptionCopier = exceptionCopier;
        _serializerSessionPool = serializerSessionPool;
        manager.RegisterStateMachine(key, this);
    }

    public bool TrySetResult(T value)
    {
        if (_status is not DurableTaskCompletionSourceStatusFixed.Pending)
        {
            return false;
        }

        _status = DurableTaskCompletionSourceStatusFixed.Completed;
        _value = _copier.Copy(value);
        return true;
    }

    public bool TrySetException(Exception exception)
    {
        if (_status is not DurableTaskCompletionSourceStatusFixed.Pending)
        {
            return false;
        }

        _status = DurableTaskCompletionSourceStatusFixed.Faulted;
        _exception = _exceptionCopier.Copy(exception);
        return true;
    }

    public bool TrySetCanceled()
    {
        if (_status is not DurableTaskCompletionSourceStatusFixed.Pending)
        {
            return false;
        }

        _status = DurableTaskCompletionSourceStatusFixed.Canceled;
        return true;
    }

    public Task<T> Task => _completion.Task;

    public DurableTaskCompletionSourceStateFixed<T> State => _status switch
    {
        DurableTaskCompletionSourceStatusFixed.Pending => new DurableTaskCompletionSourceStateFixed<T> { Status = DurableTaskCompletionSourceStatusFixed.Pending },
        DurableTaskCompletionSourceStatusFixed.Completed => new DurableTaskCompletionSourceStateFixed<T> { Status = DurableTaskCompletionSourceStatusFixed.Completed, Value = _value },
        DurableTaskCompletionSourceStatusFixed.Faulted => new DurableTaskCompletionSourceStateFixed<T> { Status = DurableTaskCompletionSourceStatusFixed.Faulted, Exception = _exception },
        DurableTaskCompletionSourceStatusFixed.Canceled => new DurableTaskCompletionSourceStateFixed<T> { Status = DurableTaskCompletionSourceStatusFixed.Canceled },
        _ => throw new InvalidOperationException($"Unexpected status, \"{_status}\""),
    };

    private void OnValuePersisted()
    {
        switch (_status)
        {
            case DurableTaskCompletionSourceStatusFixed.Completed:
                _completion.TrySetResult(_value!);
                break;
            case DurableTaskCompletionSourceStatusFixed.Faulted:
                _completion.TrySetException(_exception!);
                break;
            case DurableTaskCompletionSourceStatusFixed.Canceled:
                _completion.TrySetCanceled();
                break;
            default:
                break;
        }
    }

    void IDurableStateMachine.OnRecoveryCompleted() => OnValuePersisted();
    void IDurableStateMachine.OnWriteCompleted() => OnValuePersisted();

    void IDurableStateMachine.Reset(IStateMachineLogWriter storage)
    {
        // Reset the task completion source if necessary.
        if (_completion.Task.IsCompleted)
        {
            _completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
        }

        _storage = storage;
    }

    void IDurableStateMachine.Apply(ReadOnlySequence<byte> logEntry)
    {
        using var session = _serializerSessionPool.GetSession();
        var reader = Reader.Create(logEntry, session);
        var version = reader.ReadByte();
        if (version != SupportedVersion)
        {
            throw new NotSupportedException($"This instance of {nameof(DurableTaskCompletionSourceFixed<T>)} supports version {(uint)SupportedVersion} and not version {(uint)version}.");
        }

        _status = (DurableTaskCompletionSourceStatusFixed)reader.ReadByte();
        switch (_status)
        {
            case DurableTaskCompletionSourceStatusFixed.Completed:
                _value = ReadValue(ref reader);
                break;
            case DurableTaskCompletionSourceStatusFixed.Faulted:
                _exception = ReadException(ref reader);
                break;
            default:
                break;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        T ReadValue(ref Reader<ReadOnlySequenceInput> reader)
        {
            var field = reader.ReadFieldHeader();
            return _codec.ReadValue(ref reader, field);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        Exception ReadException(ref Reader<ReadOnlySequenceInput> reader)
        {
            var field = reader.ReadFieldHeader();
            return _exceptionCodec.ReadValue(ref reader, field);
        }
    }

    void IDurableStateMachine.AppendEntries(StateMachineStorageWriter logWriter)
    {
        if (_status is not DurableTaskCompletionSourceStatusFixed.Pending)
        {
            WriteState(logWriter);
        }
    }

    void IDurableStateMachine.AppendSnapshot(StateMachineStorageWriter snapshotWriter) => WriteState(snapshotWriter);

    private void WriteState(StateMachineStorageWriter writer)
    {
        writer.AppendEntry(static (self, bufferWriter) =>
        {
            using var session = self._serializerSessionPool.GetSession();
            var writer = Writer.Create(bufferWriter, session);
            writer.WriteByte(DurableTaskCompletionSourceFixed<T>.SupportedVersion);
            var status = self._status;
            writer.WriteByte((byte)status);
            if (status is DurableTaskCompletionSourceStatusFixed.Completed)
            {
                self._codec.WriteField(ref writer, 0, typeof(T), self._value!);
            }
            else if (status is DurableTaskCompletionSourceStatusFixed.Faulted)
            {
                self._exceptionCodec.WriteField(ref writer, 0, typeof(Exception), self._exception!);
            }

            writer.Commit();
        }, this);
    }

    public IDurableStateMachine DeepCopy() => throw new NotImplementedException();
}

[GenerateSerializer]
public enum DurableTaskCompletionSourceStatusFixed : byte
{
    Pending = 0,
    Completed,
    Faulted,
    Canceled
}

[GenerateSerializer, Immutable]
public readonly struct DurableTaskCompletionSourceStateFixed<T>
{
    [Id(0)]
    public DurableTaskCompletionSourceStatusFixed Status { get; init; }

    [Id(1)]
    public T? Value { get; init; }

    [Id(2)]
    public Exception? Exception { get; init; }
}

