// xUnit Quality Gates (Concurrency Invariants, Saga Compensation & AI Guardrails)
using System.Text;
using System.Threading.Channels;
using Microsoft.Extensions.Logging.Abstractions;
using Payments.Api.Pipelines;
using Payments.Api.Sagas;
using Xunit;

namespace Payments.Tests;

public sealed class PaymentSagaAndPipelineTests
{
    [Fact]
    public async Task ConcurrentReservations_ShouldNeverOverdrawPayerBalance()
    {
        var store = new InMemoryPaymentLedgerStore();
        var payerId = Guid.NewGuid();
        store.SeedPayerBalance(payerId, 1000m); // Only enough for 10 claims of $100

        var orchestrator = new ClaimPaymentSagaOrchestrator(
            store,
            new AlwaysApproveClearinghouse(),
            NullLogger<ClaimPaymentSagaOrchestrator>.Instance);

        // Fire 25 concurrent claims of $100 each against a $1,000 balance
        var tasks = Enumerable.Range(0, 25).Select(i =>
            orchestrator.ExecuteAsync(
                new ClaimPaymentCommand(Guid.NewGuid(), payerId, "1234567890", 100m, $"trace-{i}"),
                CancellationToken.None));

        var results = await Task.WhenAll(tasks);

        Assert.Equal(10, results.Count(r => r.State == SagaState.Settled));
        Assert.Equal(15, results.Count(r => r.State == SagaState.Compensated));
        Assert.Equal(0m, store.GetBalance(payerId));
    }

    [Fact]
    public async Task ClearinghouseFailure_ShouldTriggerCompensatingRefund()
    {
        var store = new InMemoryPaymentLedgerStore();
        var payerId = Guid.NewGuid();
        store.SeedPayerBalance(payerId, 5000m);

        var orchestrator = new ClaimPaymentSagaOrchestrator(
            store,
            new AlwaysRejectClearinghouse(),
            NullLogger<ClaimPaymentSagaOrchestrator>.Instance);

        var result = await orchestrator.ExecuteAsync(
            new ClaimPaymentCommand(Guid.NewGuid(), payerId, "1234567890", 1500m, "trace-comp-1"),
            CancellationToken.None);

        Assert.Equal(SagaState.Compensated, result.State);
        Assert.Equal(5000m, store.GetBalance(payerId)); // Funds fully restored
    }

    [Fact]
    public async Task PipelineIngestor_ShouldStreamAndParseNdjsonIntoBoundedChannel()
    {
        var channel = Channel.CreateBounded<ClaimPaymentCommand>(100);
        var ingestor = new ClaimStreamIngestor(channel.Writer);

        var ndjson = new StringBuilder()
            .AppendLine($"{{\"claimId\":\"{Guid.NewGuid()}\",\"payerId\":\"{Guid.NewGuid()}\",\"providerNpi\":\"1234567890\",\"amount\":250.50}}")
            .AppendLine($"{{\"claimId\":\"{Guid.NewGuid()}\",\"payerId\":\"{Guid.NewGuid()}\",\"providerNpi\":\"0987654321\",\"amount\":900.00}}")
            .ToString();

        await using var stream = new MemoryStream(Encoding.UTF8.GetBytes(ndjson));
        int count = await ingestor.IngestStreamAsync(stream, CancellationToken.None);

        Assert.Equal(2, count);
        Assert.True(channel.Reader.TryRead(out var first));
        Assert.Equal(250.50m, first!.Amount);
    }

    private sealed class AlwaysApproveClearinghouse : IClearinghouseGateway
    {
        public Task<bool> SettleAchAsync(Guid sagaId, string providerNpi, decimal amount, CancellationToken ct) => Task.FromResult(true);
    }

    private sealed class AlwaysRejectClearinghouse : IClearinghouseGateway
    {
        public Task<bool> SettleAchAsync(Guid sagaId, string providerNpi, decimal amount, CancellationToken ct) => Task.FromResult(false);
    }
}
