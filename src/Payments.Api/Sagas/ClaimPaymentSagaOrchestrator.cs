// Saga Orchestrator (Reservation -> Clearinghouse Settlement -> Compensation)
using System.Collections.Concurrent;
using System.Diagnostics;
using Payments.Api.Guardrails;
using Payments.Api.Pipelines;

namespace Payments.Api.Sagas;

public enum SagaState { Reserved = 1, Settled = 2, Compensated = 3 }

public sealed record SagaExecutionRecord(
    Guid SagaId,
    Guid ClaimId,
    decimal Amount,
    SagaState State,
    string TraceId,
    string? FailureReason = null);

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

    public async Task<SagaExecutionRecord> ExecuteAsync(ClaimPaymentCommand cmd, CancellationToken ct)
    {
        using var activity = Telemetry.StartActivity("ExecuteClaimPaymentSaga");
        activity?.SetTag("claim.id", cmd.ClaimId);
        activity?.SetTag("trace.id", cmd.TraceId);

        // 1. Idempotency & Atomic Reservation inside Payer boundary
        var reservation = await ledgerStore.TryReserveFundsAsync(cmd, ct);
        if (!reservation.Success)
        {
            logger.LogWarning("Trace={TraceId} Claim={ClaimId} rejected: {Reason}",
                cmd.TraceId, cmd.ClaimId, reservation.Error);
            return new SagaExecutionRecord(Guid.Empty, cmd.ClaimId, cmd.Amount, SagaState.Compensated, cmd.TraceId, reservation.Error);
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
        bool settled = await clearinghouse.SettleAchAsync(reservation.SagaId, cmd.ProviderNpi, cmd.Amount, ct);
        if (!settled)
        {
            await ledgerStore.CompensateReservationAsync(reservation.SagaId, cmd.PayerId, cmd.Amount, ct);
            logger.LogError("Trace={TraceId} Claim={ClaimId} clearinghouse failed; compensating release executed.",
                cmd.TraceId, cmd.ClaimId);
            return new SagaExecutionRecord(reservation.SagaId, cmd.ClaimId, cmd.Amount, SagaState.Compensated, cmd.TraceId, "Clearinghouse settlement rejected");
        }

        await ledgerStore.MarkSettledAsync(reservation.SagaId, ct);
        logger.LogInformation("Trace={TraceId} Claim={ClaimId} Saga={SagaId} settled for ${Amount}",
            cmd.TraceId, cmd.ClaimId, reservation.SagaId, cmd.Amount);

        return new SagaExecutionRecord(reservation.SagaId, cmd.ClaimId, cmd.Amount, SagaState.Settled, cmd.TraceId);
    }
}

public interface IPaymentLedgerStore
{
    Task<(bool Success, Guid SagaId, string? Error)> TryReserveFundsAsync(ClaimPaymentCommand cmd, CancellationToken ct);
    Task CompensateReservationAsync(Guid sagaId, Guid payerId, decimal amount, CancellationToken ct);
    Task MarkSettledAsync(Guid sagaId, CancellationToken ct);
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
    private readonly object _lock = new();

    public void SeedPayerBalance(Guid payerId, decimal balance) => _balances[payerId] = balance;
    public decimal GetBalance(Guid payerId) => _balances.GetValueOrDefault(payerId, 0m);
    public SagaState GetSagaState(Guid sagaId) => _sagaStates[sagaId];

    public Task<(bool Success, Guid SagaId, string? Error)> TryReserveFundsAsync(ClaimPaymentCommand cmd, CancellationToken ct)
    {
        lock (_lock)
        {
            if (_idempotencyIndex.TryGetValue(cmd.ClaimId, out var existingSagaId))
                return Task.FromResult((true, existingSagaId, (string?)null));

            decimal current = _balances.GetValueOrDefault(cmd.PayerId, 0m);
            if (current < cmd.Amount)
                return Task.FromResult((false, Guid.Empty, (string?)"Insufficient payer liquidity"));

            _balances[cmd.PayerId] = current - cmd.Amount;
            var sagaId = Guid.NewGuid();
            _idempotencyIndex[cmd.ClaimId] = sagaId;
            _sagaStates[sagaId] = SagaState.Reserved;
            return Task.FromResult((true, sagaId, (string?)null));
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

    public Task MarkSettledAsync(Guid sagaId, CancellationToken ct)
    {
        _sagaStates[sagaId] = SagaState.Settled;
        return Task.CompletedTask;
    }
}
