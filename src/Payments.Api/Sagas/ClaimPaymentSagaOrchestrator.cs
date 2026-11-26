// Saga Orchestrator (Reservation -> Clearinghouse Settlement -> Compensation)
using System.Collections.Concurrent;
using System.Diagnostics;
using Payments.Api.Guardrails;
using Payments.Api.Pipelines;
using Polly.CircuitBreaker;

namespace Payments.Api.Sagas;

public enum SagaState { Reserved = 1, Settled = 2, Compensated = 3 }

public sealed record SagaExecutionRecord(
    Guid SagaId,
    Guid ClaimId,
    decimal Amount,
    SagaState State,
    string TraceId,
    string? FailureReason = null,
    bool IsTransient = false);

/// <summary>
/// Orchestrates multi-step Healthcare Payment settlement without distributed 2PC locks:
/// Step 1: Atomic Payer Liquidity Reservation (Idempotent by ClaimId)
/// Step 2: Deterministic AI/Policy Risk Guardrail Check
/// Step 3: External Clearinghouse ACH/RTP Settlement -> Executes Compensating Refund on Failure
/// </summary>
public sealed class ClaimPaymentSagaOrchestrator(
    IPaymentLedgerStore ledgerStore,
    IClearinghouseGateway clearinghouse,
    ILogger<ClaimPaymentSagaOrchestrator> logger)
{
    public static readonly ActivitySource Telemetry = new("COMPANYNAME.HealthcarePayments.Saga");

    private readonly ConcurrentDictionary<Guid, Lazy<Task<SagaExecutionRecord>>> _inFlight = new();

    public async Task<SagaExecutionRecord> ExecuteAsync(ClaimPaymentCommand cmd, CancellationToken ct)
    {
        // Single-flight: concurrent duplicates for the same ClaimId collapse into one execution.
        var lazy = _inFlight.GetOrAdd(
            cmd.ClaimId,
            _ => new Lazy<Task<SagaExecutionRecord>>(
                () => ExecuteCoreAsync(cmd, ct),
                LazyThreadSafetyMode.ExecutionAndPublication));

        try
        {
            return await lazy.Value.WaitAsync(ct).ConfigureAwait(false);
        }
        finally
        {
            // Only evict once the shared execution has completed, and only this exact entry,
            // so a newer in-flight registration is never removed by a late waiter.
            if (lazy.IsValueCreated && lazy.Value.IsCompleted)
                _inFlight.TryRemove(new KeyValuePair<Guid, Lazy<Task<SagaExecutionRecord>>>(cmd.ClaimId, lazy));
        }
    }

    private async Task<SagaExecutionRecord> ExecuteCoreAsync(ClaimPaymentCommand cmd, CancellationToken ct)
    {
        using var activity = Telemetry.StartActivity("ExecuteClaimPaymentSaga");
        activity?.SetTag("claim.id", cmd.ClaimId);
        activity?.SetTag("trace.id", cmd.TraceId);

        // 1. Idempotency & Atomic Reservation inside Payer boundary
        var reservation = await ledgerStore.TryReserveFundsAsync(cmd, ct);

        // A previously settled claim is a pure replay: return the stored outcome without
        // re-running the guardrail or issuing a second ACH settlement.
        if (reservation.Success && reservation.CurrentState == SagaState.Settled)
        {
            var existing = await ledgerStore.GetSettledRecordAsync(reservation.SagaId, ct);
            if (existing is not null)
            {
                logger.LogInformation("Trace={TraceId} Claim={ClaimId} Saga={SagaId} replay of settled claim; returning existing record.",
                    cmd.TraceId, cmd.ClaimId, reservation.SagaId);
                return existing;
            }

            logger.LogInformation("Trace={TraceId} Claim={ClaimId} Saga={SagaId} replay of settled claim.",
                cmd.TraceId, cmd.ClaimId, reservation.SagaId);
            return new SagaExecutionRecord(reservation.SagaId, cmd.ClaimId, cmd.Amount, SagaState.Settled, cmd.TraceId);
        }

        if (!reservation.Success)
        {
            logger.LogWarning("Trace={TraceId} Claim={ClaimId} rejected: {Reason}",
                cmd.TraceId, cmd.ClaimId, reservation.Error);
            return new SagaExecutionRecord(reservation.SagaId, cmd.ClaimId, cmd.Amount, reservation.CurrentState, cmd.TraceId, reservation.Error);
        }

        // 2. Deterministic AI/Compliance Invariant Guardrail
        var audit = PaymentComplianceGuardrail.Validate(cmd);
        if (!audit.IsCompliant)
        {
            await ledgerStore.CompensateReservationAsync(reservation.SagaId, cmd.PayerId, cmd.Amount, ct);
            logger.LogWarning("Trace={TraceId} Claim={ClaimId} compensated due to compliance rule: {Reason}",
                cmd.TraceId, cmd.ClaimId, audit.ViolationReason);
            return new SagaExecutionRecord(reservation.SagaId, cmd.ClaimId, cmd.Amount, SagaState.Compensated, cmd.TraceId, audit.ViolationReason);
        }

        // 3. External Clearinghouse Settlement (with Compensating Action on failure)
        bool settled;
        try
        {
            settled = await clearinghouse.SettleAchAsync(reservation.SagaId, cmd.ProviderNpi, cmd.Amount, ct);
        }
        catch (BrokenCircuitException ex)
        {
            // The ACH circuit is open: fail fast and release the semantic lock immediately.
            await ledgerStore.CompensateReservationAsync(reservation.SagaId, cmd.PayerId, cmd.Amount, ct);
            logger.LogWarning(ex,
                "Trace={TraceId} Claim={ClaimId} clearinghouse circuit open; compensating release executed.",
                cmd.TraceId, cmd.ClaimId);
            return new SagaExecutionRecord(reservation.SagaId, cmd.ClaimId, cmd.Amount, SagaState.Compensated,
                cmd.TraceId, "Clearinghouse circuit open; reservation compensated", IsTransient: true);
        }

        if (!settled)
        {
            await ledgerStore.CompensateReservationAsync(reservation.SagaId, cmd.PayerId, cmd.Amount, ct);
            logger.LogError("Trace={TraceId} Claim={ClaimId} clearinghouse failed; compensating release executed.",
                cmd.TraceId, cmd.ClaimId);
            return new SagaExecutionRecord(reservation.SagaId, cmd.ClaimId, cmd.Amount, SagaState.Compensated, cmd.TraceId, "Clearinghouse settlement rejected");
        }

        var settledRecord = new SagaExecutionRecord(reservation.SagaId, cmd.ClaimId, cmd.Amount, SagaState.Settled, cmd.TraceId);
        await ledgerStore.MarkSettledAsync(settledRecord, ct);
        logger.LogInformation("Trace={TraceId} Claim={ClaimId} Saga={SagaId} settled for ${Amount}",
            cmd.TraceId, cmd.ClaimId, reservation.SagaId, cmd.Amount);

        return settledRecord;
    }
}

public interface IPaymentLedgerStore
{
    Task<(bool Success, Guid SagaId, SagaState CurrentState, string? Error)> TryReserveFundsAsync(ClaimPaymentCommand cmd, CancellationToken ct);
    Task CompensateReservationAsync(Guid sagaId, Guid payerId, decimal amount, CancellationToken ct);
    Task MarkSettledAsync(SagaExecutionRecord record, CancellationToken ct);
    Task<SagaExecutionRecord?> GetSettledRecordAsync(Guid sagaId, CancellationToken ct);
}

public interface IClearinghouseGateway
{
    Task<bool> SettleAchAsync(Guid sagaId, string providerNpi, decimal amount, CancellationToken ct);
}

/// <summary>Thread-safe in-memory ledger store for local demo & deterministic unit testing.</summary>
public sealed class InMemoryPaymentLedgerStore : IPaymentLedgerStore
{
    private readonly ConcurrentDictionary<Guid, decimal> _balances = new();
    private readonly ConcurrentDictionary<Guid, Guid> _idempotencyIndex = new();
    private readonly ConcurrentDictionary<Guid, SagaState> _sagaStates = new();
    private readonly ConcurrentDictionary<Guid, SagaExecutionRecord> _settledRecords = new();
    private readonly Lock _lock = new();

    public void SeedPayerBalance(Guid payerId, decimal balance) => _balances[payerId] = balance;
    public decimal GetBalance(Guid payerId) => _balances.GetValueOrDefault(payerId, 0m);
    public SagaState GetSagaState(Guid sagaId) => _sagaStates[sagaId];

    public Task<(bool Success, Guid SagaId, SagaState CurrentState, string? Error)> TryReserveFundsAsync(ClaimPaymentCommand cmd, CancellationToken ct)
    {
        lock (_lock)
        {
            if (_idempotencyIndex.TryGetValue(cmd.ClaimId, out var existingSagaId))
            {
                var existingState = _sagaStates.GetValueOrDefault(existingSagaId, SagaState.Reserved);

                // Already settled: the caller must return the stored outcome without re-settling.
                if (existingState == SagaState.Settled)
                    return Task.FromResult((true, existingSagaId, SagaState.Settled, (string?)null));

                // In-flight: reject concurrent duplicate execution for the same claim.
                if (existingState == SagaState.Reserved)
                    return Task.FromResult((false, existingSagaId, SagaState.Reserved, (string?)"Settlement already in progress for ClaimId"));

                // Compensated: the prior attempt released its funds, so a retry reserves cleanly.
                _idempotencyIndex.TryRemove(cmd.ClaimId, out _);
            }

            decimal current = _balances.GetValueOrDefault(cmd.PayerId, 0m);
            if (current < cmd.Amount)
                return Task.FromResult((false, Guid.Empty, SagaState.Compensated, (string?)"Insufficient payer liquidity"));

            _balances[cmd.PayerId] = current - cmd.Amount;
            var sagaId = Guid.NewGuid();
            _idempotencyIndex[cmd.ClaimId] = sagaId;
            _sagaStates[sagaId] = SagaState.Reserved;
            return Task.FromResult((true, sagaId, SagaState.Reserved, (string?)null));
        }
    }

    public Task CompensateReservationAsync(Guid sagaId, Guid payerId, decimal amount, CancellationToken ct)
    {
        lock (_lock)
        {
            if (_sagaStates.TryGetValue(sagaId, out var state) && state == SagaState.Reserved)
            {
                _balances[payerId] = _balances.GetValueOrDefault(payerId, 0m) + amount;
                _sagaStates[sagaId] = SagaState.Compensated;
            }
        }
        return Task.CompletedTask;
    }

    public Task MarkSettledAsync(SagaExecutionRecord record, CancellationToken ct)
    {
        lock (_lock)
        {
            _sagaStates[record.SagaId] = SagaState.Settled;
            _settledRecords[record.SagaId] = record;
        }
        return Task.CompletedTask;
    }

    public Task<SagaExecutionRecord?> GetSettledRecordAsync(Guid sagaId, CancellationToken ct)
    {
        _settledRecords.TryGetValue(sagaId, out var record);
        return Task.FromResult(record);
    }
}
