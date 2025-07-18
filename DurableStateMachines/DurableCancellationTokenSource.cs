using System.Buffers;
using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;

namespace Ledjon.DurableStateMachines;

/// <summary>
/// Represents the producer side of a durable cancellation, which can be persisted and recovered across activations.
/// </summary>
public interface IDurableCancellationTokenSource
{
    /// <summary>
    /// Gets the <see cref="CancellationToken"/> associated with this source.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This token is signaled only <strong>after</strong> a cancellation request has been durably persisted.
    /// It represents the <strong>committed</strong> cancellation state. For the immediate, in-memory status, check the
    /// <see cref="IsCancellationPending"/> property.
    /// </para>
    /// <para>
    /// Any callbacks registered via <see cref="CancellationToken.Register(Action)"/> are executed <strong>after</strong>
    /// the cancellation request has been durably persisted.
    /// </para>
    /// <para>
    /// Any callbacks registered via <see cref="CancellationToken.Register(Action)"/> are executed on the <see cref="TaskScheduler.Default"/>.
    /// In the majority of the cases this will be the <see cref="ThreadPool"/>, in other words the callbacks will execute <strong>outside</strong> the grain context.
    /// </para>
    /// </remarks>
    CancellationToken Token { get; }

    /// <summary>
    /// Gets a value indicating whether cancellation is pending for this source.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This property returns <see langword="true"/> if <see cref="Cancel()"/> has been called, or if the delay
    /// from a prior <see cref="CancelAfter(TimeSpan)"/> call has elapsed.
    /// </para>
    /// <para>
    /// This value reflects the current computed state and can be <see langword="true"/> <strong>before</strong> the state has been
    /// durably persisted. It is useful for checking the cancellation status before the state is saved durably.
    /// </para>
    /// </remarks>
    bool IsCancellationPending { get; }

    /// <summary>
    /// Communicates a request for cancellation.
    /// </summary>
    /// <remarks>
    /// This method only updates the in-memory state. The cancellation request is not durable and will be lost
    /// on deactivation until an explicit persistence call is successfully completed.
    /// Upon successful persistence, the <see cref="Token"/> will be signaled.
    /// </remarks>
    void Cancel();

    /// <summary>
    /// Schedules a cancellation request to occur after <paramref name="delay"/> has elapsed.
    /// </summary>
    /// <param name="delay">The time interval to wait before canceling. Must be a non-negative value.</param>
    /// <remarks>
    /// <para>
    /// This method initiates a two-step cancellation process that distinguishes it from <see cref="Cancel()"/>:
    /// </para>
    /// <para>
    /// <strong>Persisting the Intent:</strong> This method only records the <strong>intent</strong> to cancel in memory.
    /// To make this scheduled cancellation durable, you must subsequently call <i>WriteStateAsync</i>.
    /// If this is not done, the scheduled cancellation will be lost upon deactivation.
    /// </para>
    /// <para>
    /// <strong>Automatic Persistence on Expiration:</strong> Once the <paramref name="delay"/> expires, this
    /// component will automatically trigger an internal process to durably persist the final state.
    /// This is a key difference from <see cref="Cancel()"/>, which requires a <strong>manual</strong> persistence call to
    /// finalize the cancellation.
    /// </para>
    /// <para>
    /// <strong>Durability Across Deactivation:</strong> If the host is deactivated after the intent has been persisted
    /// but before the <paramref name="delay"/> elapses, the state machine guarantees that the remaining time will be
    /// honored upon the next activation, after which the final state will be automatically persisted.
    /// </para>
    /// <para>
    /// <strong>Behavior Notes:</strong> If this method is called multiple times, only the request with the earliest
    /// cancellation time is honored. A subsequent call with a longer delay will not override an existing, shorter delay.
    /// A delay of <see cref="TimeSpan.Zero"/> will immediately trigger the automatic persistence of the state,
    /// behaving like an auto-committing version of <see cref="Cancel()"/>.
    /// </para>
    /// </remarks>
    void CancelAfter(TimeSpan delay);
}

[DebuggerDisplay("IsCanceled = {IsCancellationRequested}")]
internal sealed class DurableCancellationTokenSource : IDurableCancellationTokenSource, IDurableStateMachine, IDisposable
{
    private const byte VersionByte = 0;

    private bool _isCanceled;
    private ScheduledCancellation _scheduledCancellation;

    private CancellationTokenRegistration _ctsr;
    private CancellationTokenSource _cts = new();

    private readonly ReaderWriterLockSlim _lock = new();
    private readonly TimeProvider _timeProvider;
    private readonly SerializerSessionPool _sessionPool;
    private readonly IStateMachineManager _manager;

    private struct ScheduledCancellation
    {
        /// <summary>
        /// When the request was made.
        /// </summary>
        public long RequestTicks;

        /// <summary>
        /// The original delay requested.
        /// </summary>
        public long DelayTicks;

        public readonly bool IsScheduled => RequestTicks != -1;
        public readonly long ExpirationTicks => RequestTicks + DelayTicks;

        /// <summary>
        /// If a scheduled cancellation exists, this resets/invalidates it.
        /// </summary>
        public void Reset()
        {
            DelayTicks = -1;
            RequestTicks = -1;
        }
    }

    public DurableCancellationTokenSource(
        [ServiceKey] string key, IStateMachineManager manager,
        TimeProvider timeProvider, SerializerSessionPool sessionPool)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);

        _timeProvider = timeProvider;
        _sessionPool = sessionPool;
        _manager = manager;

        _ctsr = ObserveCancellation();
        _scheduledCancellation.Reset();
        _manager.RegisterStateMachine(key, this);
    }

    public CancellationToken Token => _cts.Token;

    public bool IsCancellationPending
    {
        get
        {
            _lock.EnterReadLock();

            try
            {
                if (_isCanceled)
                {
                    return true;
                }

                if (!_scheduledCancellation.IsScheduled)
                {
                    return false;
                }

                // We calculate the expiration time based on the original request and check if it has passed.
                // This returns true if the calculated expiration time is less than, or equal to the current time,
                // meaning the scheduled cancellation moment has been reached or has passed.

                return _scheduledCancellation.ExpirationTicks <= _timeProvider.GetUtcNow().UtcTicks;
            }
            finally
            {
                _lock.ExitReadLock();
            }
        }
    }

    public void Cancel()
    {
        _lock.EnterWriteLock();

        try
        {
            _isCanceled = true;
            _scheduledCancellation.Reset();
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    public void CancelAfter(TimeSpan delay)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(delay, TimeSpan.Zero, nameof(delay));

        var utcNowTicks = _timeProvider.GetUtcNow().UtcTicks;
        var newExpirationTicks = utcNowTicks + delay.Ticks;

        _lock.EnterWriteLock();

        try
        {
            // We only set the new schedule if there is none, or if it is sooner than the existing one.
            if (!_scheduledCancellation.IsScheduled || newExpirationTicks < _scheduledCancellation.ExpirationTicks)
            {
                _scheduledCancellation.RequestTicks = utcNowTicks;
                _scheduledCancellation.DelayTicks = delay.Ticks;

                _cts.CancelAfter(delay);
            }
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    public void Dispose()
    {
        _lock.EnterWriteLock();

        try
        {
            _ctsr.Dispose();
            _cts.Dispose();
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    /// <remarks>
    /// <para>
    /// We register ourselves as a callback, in addition to potential other sources.
    /// When a scheduled cancellation is requested, we need to update our in-memory state
    /// (durability comes after WriteStateAsync is called) to reflect the scheduled cancellation has occurred.
    /// </para>
    /// <para>
    /// We use <see cref="CancellationToken.UnsafeRegister"/> because the <see cref="IStateMachineManager"/>
    /// already suppresses the execution context, so there is no context to flow anyway.
    /// </para>
    /// </remarks>
    private CancellationTokenRegistration ObserveCancellation() =>
        Token.UnsafeRegister(static async state =>
        {
            var self = (DurableCancellationTokenSource)state!;
            var rwLock = self._lock;

            // We backup the state fields in case the write fails.

            bool isCanceled;
            ScheduledCancellation scheduled;

            rwLock.EnterReadLock();

            try
            {
                isCanceled = self._isCanceled;
                scheduled = self._scheduledCancellation;
            }
            finally
            {
                rwLock.ExitReadLock();
            }

            self.Cancel(); // This sets: _isCanceled = true, and resets _scheduledCancellation too.

            try
            {
                await self._manager.WriteStateAsync(CancellationToken.None);
            }
            catch
            {
                // Something was wrong with persisting, so we restore the state (undo Cancel), conditionally!
                rwLock.EnterWriteLock();

                try
                {
                    // We should only rollback if the state is still the one we set above.
                    // When we call WriteStateAsync, we yield back, and a call to CancelAfter could update the state.
                    // In that case, we should not rollback, as we would be destroying a more recent state change.

                    if (self._isCanceled && !self._scheduledCancellation.IsScheduled)
                    {
                        // The state is still the one we set, so we perform the rollback now.
                        self._isCanceled = isCanceled;
                        self._scheduledCancellation = scheduled;
                    }
                }
                finally
                {
                    rwLock.ExitWriteLock();
                }
            }
        }, this);

    void IDurableStateMachine.OnWriteCompleted()
    {
        if (IsCancellationPending)
        {
            _cts.Cancel();
        }
    }

    void IDurableStateMachine.OnRecoveryCompleted()
    {
        if (IsCancellationPending)
        {
            _cts.Cancel();
            return;
        }

        if (_scheduledCancellation.IsScheduled)
        {
            var timeElapsedTicks = _timeProvider.GetUtcNow().UtcTicks - _scheduledCancellation.RequestTicks;
            var remainingTicks = _scheduledCancellation.DelayTicks - timeElapsedTicks;

            if (remainingTicks <= 0)
            {
                _cts.Cancel(); // The scheduled time has passed while deactivated. Trigger cancellation now.
            }
            else
            {
                _cts.CancelAfter(TimeSpan.FromTicks(remainingTicks)); // The scheduled time is still in the future. Re-arm the in-memory timer.
            }
        }
    }

    void IDurableStateMachine.Reset(IStateMachineLogWriter storage)
    {
        if (_cts.IsCancellationRequested)
        {
            _ctsr.Dispose();
            _cts.Dispose();

            // It is best we crate a new CTS upon reseting, because we publicly expose cts.Token, and we cannot "un-cancel" its source
            // without violating the contract with consumers who may have registered callbacks, or have passed the token to other operations.

            _cts = new CancellationTokenSource();
            _ctsr = ObserveCancellation();
        }

        _isCanceled = false;
        _scheduledCancellation.Reset();
    }

    void IDurableStateMachine.Apply(ReadOnlySequence<byte> logEntry)
    {
        using var session = _sessionPool.GetSession();

        var reader = Reader.Create(logEntry, session);
        var version = reader.ReadByte();

        if (version != VersionByte)
        {
            throw new NotSupportedException($"This instance of {nameof(DurableCancellationTokenSource)} supports version {VersionByte} and not version {version}.");
        }

        _isCanceled = reader.ReadByte() == 1;
        var isScheduled = reader.ReadByte() == 1;

        if (isScheduled)
        {
            _scheduledCancellation.RequestTicks = reader.ReadInt64();
            _scheduledCancellation.DelayTicks = reader.ReadInt64();
        }
        else
        {
            _scheduledCancellation.Reset();
        }
    }

    void IDurableStateMachine.AppendEntries(StateMachineStorageWriter writer)
    {
        if (_isCanceled || _scheduledCancellation.IsScheduled)
        {
            // We apply the write operation only if we did *change* our default state i.e:
            // non-canceled & no cancellation scheduled, making this slightly more space efficient.

            ApplyWrite(writer);
        }
    }

    void IDurableStateMachine.AppendSnapshot(StateMachineStorageWriter writer)
    {
        ApplyWrite(writer);
    }

    private void ApplyWrite(StateMachineStorageWriter writer)
    {
        writer.AppendEntry(static (self, bufferWriter) =>
        {
            using var session = self._sessionPool.GetSession();

            var writer = Writer.Create(bufferWriter, session);

            writer.WriteByte(VersionByte);
            writer.WriteByte((byte)(self._isCanceled ? 1 : 0));

            var isScheduled = self._scheduledCancellation.IsScheduled;
            writer.WriteByte((byte)(isScheduled ? 1 : 0));

            if (isScheduled)
            {
                writer.WriteInt64(self._scheduledCancellation.RequestTicks);
                writer.WriteInt64(self._scheduledCancellation.DelayTicks);
            }

            writer.Commit();
        }, this);
    }

    public IDurableStateMachine DeepCopy() => throw new NotImplementedException();
}