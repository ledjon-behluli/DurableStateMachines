using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

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

/// <summary>
/// Receives decoded durable cancellation token source commands from a codec implementation.
/// </summary>
public interface IDurableCancellationTokenSourceCommandHandler
{
    /// <summary>Applies a state command.</summary>
    void ApplyState(bool isCanceled, bool isScheduled, long requestTicks, long delayTicks);

    /// <summary>Resets the receiver before applying replacement entries.</summary>
    void Reset();
}

/// <summary>
/// Serializes one durable cancellation token source command and applies one decoded command.
/// </summary>
public interface IDurableCancellationTokenSourceCommandCodec
{
    /// <summary>Writes a state command.</summary>
    void WriteState(bool isCanceled, bool isScheduled, long requestTicks, long delayTicks, JournalStreamWriter writer);

    /// <summary>Reads one encoded command and applies it to <paramref name="handler"/>.</summary>
    void Apply(JournalBufferReader input, IDurableCancellationTokenSourceCommandHandler handler);
}

internal sealed class DurableCancellationTokenSourceCommandBinaryCodec(
    SerializerSessionPool sessionPool) : IDurableCancellationTokenSourceCommandCodec
{
    private const byte VersionByte = 0;

    public void WriteState(bool isCanceled, bool isScheduled, long requestTicks, long delayTicks, JournalStreamWriter writer)
    {
        using var entry = writer.BeginEntry();
        using var session = sessionPool.GetSession();

        var payloadWriter = Writer.Create(entry.Writer, session);

        payloadWriter.WriteByte(VersionByte);
        payloadWriter.WriteByte((byte)(isCanceled ? 1 : 0));
        payloadWriter.WriteByte((byte)(isScheduled ? 1 : 0));

        if (isScheduled)
        {
            payloadWriter.WriteInt64(requestTicks);
            payloadWriter.WriteInt64(delayTicks);
        }

        payloadWriter.Commit();
        entry.Commit();
    }

    public void Apply(JournalBufferReader input, IDurableCancellationTokenSourceCommandHandler handler)
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

        var isCanceled = reader.ReadByte() == 1;
        var isScheduled = reader.ReadByte() == 1;

        long requestTicks = -1;
        long delayTicks = -1;

        if (isScheduled)
        {
            requestTicks = reader.ReadInt64();
            delayTicks = reader.ReadInt64();
        }

        handler.ApplyState(isCanceled, isScheduled, requestTicks, delayTicks);

        Helpers.ThrowIfTrailingData(ref reader);
    }
}

internal sealed class DurableCancellationTokenSource :
    IDurableCancellationTokenSource,
    IDurableCancellationTokenSourceCommandHandler,
    IJournaledState,
    IDisposable
{
    private bool _isCanceled;
    private ScheduledCancellation _scheduledCancellation;
    private CancellationTokenSource _cts;
    private CancellationTokenRegistration _ctsr;

    private readonly ReaderWriterLockSlim _lock = new();
    private readonly TimeProvider _timeProvider;
    private readonly IJournaledStateManager _manager;
    private readonly IDurableCancellationTokenSourceCommandCodec _codec;

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
        [ServiceKey] string key, IJournaledStateManager manager,
        IOptions<JournaledStateManagerOptions> options,
        TimeProvider timeProvider, IServiceProvider serviceProvider)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);

        _timeProvider = timeProvider;
        _manager = manager;
        _codec = Helpers.GetCodec<IDurableCancellationTokenSourceCommandCodec>(serviceProvider, options);
        _cts = new(Timeout.InfiniteTimeSpan, _timeProvider);
        _ctsr = ObserveCancellation();
        _scheduledCancellation.Reset();

        manager.RegisterState(key, this);
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

    #region IJournalState

    IJournaledState IJournaledState.DeepCopy() => throw new NotImplementedException();

    void IJournaledState.ReplayEntry(JournalEntry entry, JournalReplayContext context) =>
        context.GetRequiredCommandCodec(entry.FormatKey, _codec).Apply(entry.Reader, this);

    void IJournaledState.AppendEntries(JournalStreamWriter writer)
    {
        if (_isCanceled || _scheduledCancellation.IsScheduled)
        {
            // We apply the write operation only if we did *change* our default state i.e:
            // non-canceled & no cancellation scheduled, making this slightly more space efficient.

            _codec.WriteState(_isCanceled, _scheduledCancellation.IsScheduled, _scheduledCancellation.RequestTicks, _scheduledCancellation.DelayTicks, writer);
        }
    }

    void IJournaledState.AppendSnapshot(JournalStreamWriter snapshotWriter)
    {
        _codec.WriteState(_isCanceled, _scheduledCancellation.IsScheduled, _scheduledCancellation.RequestTicks, _scheduledCancellation.DelayTicks, snapshotWriter);
    }

    void IJournaledState.Reset(JournalStreamWriter writer)
    {
        if (_cts.IsCancellationRequested)
        {
            _ctsr.Dispose();
            _cts.Dispose();

            // It is best we crate a new CTS upon reseting, because we publicly expose cts.Token, and we cannot "un-cancel" its source
            // without violating the contract with consumers who may have registered callbacks, or have passed the token to other operations.

            _cts = new(Timeout.InfiniteTimeSpan, _timeProvider);
            _ctsr = ObserveCancellation();
        }

        _isCanceled = false;
        _scheduledCancellation.Reset();
    }

    void IJournaledState.OnWriteCompleted()
    {
        if (IsCancellationPending)
        {
            _cts.Cancel();
        }
    }

    void IJournaledState.OnRecoveryCompleted()
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

    #endregion

    #region IDurableCancellationTokenSourceCommandHandler

    void IDurableCancellationTokenSourceCommandHandler.ApplyState(bool isCanceled, bool isScheduled, long requestTicks, long delayTicks)
    {
        _isCanceled = isCanceled;
        if (isScheduled)
        {
            _scheduledCancellation.RequestTicks = requestTicks;
            _scheduledCancellation.DelayTicks = delayTicks;
        }
        else
        {
            _scheduledCancellation.Reset();
        }
    }

    void IDurableCancellationTokenSourceCommandHandler.Reset()
    {
        _isCanceled = false;
        _scheduledCancellation.Reset();
    }

    #endregion

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
    /// We use <see cref="CancellationToken.UnsafeRegister(Action{object?}, object?)"/> because the <see cref="IStateMachineManager"/>
    /// already suppresses the execution context, so there is no context to flow anyway.
    /// </para>
    /// </remarks>
    private CancellationTokenRegistration ObserveCancellation() =>
        Token.UnsafeRegister(static async state =>
        {
            var self = (DurableCancellationTokenSource)state!;
            var rwLock = self._lock;

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
}