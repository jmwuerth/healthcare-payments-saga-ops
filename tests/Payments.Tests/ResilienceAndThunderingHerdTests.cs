using System.Net;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Payments.Api.Hosting;
using Payments.Api.Pipelines;
using Payments.Api.Sagas;
using Xunit;

namespace Payments.Tests;

public sealed class ResilienceAndThunderingHerdTests
{
    [Fact]
    public async Task ClearinghouseOutage_ShouldTripCircuitBreakerAndCompensateFunds()
    {
        // Real resilience pipeline, stubbed transport: every attempt returns 503.
        var handler = new AlwaysUnavailableHandler();
        var services = new ServiceCollection();
        services.AddSingleton<SimulatedClearinghouseGateway>();
        services.AddSingleton<IConfiguration>(new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["Clearinghouse:BaseUrl"] = "http://ach.test"
            })
            .Build());
        services.AddHttpClient<IClearinghouseGateway, HttpClearinghouseGateway>()
            .ConfigureHttpClient(client => client.BaseAddress = new Uri("http://ach.test"))
            .ConfigurePrimaryHttpMessageHandler(() => handler)
            .AddResilienceHandler("AchClearinghouseGuard", AchResiliencePipeline.Configure);

        using var provider = services.BuildServiceProvider();

        var store = new InMemoryPaymentLedgerStore();
        var payerId = Guid.NewGuid();
        store.SeedPayerBalance(payerId, 10_000m);

        var orchestrator = new ClaimPaymentSagaOrchestrator(
            store,
            provider.GetRequiredService<IClearinghouseGateway>(),
            NullLogger<ClaimPaymentSagaOrchestrator>.Instance);

        // First claim exhausts the retries and feeds the breaker's sampling window.
        var first = await orchestrator.ExecuteAsync(
            new ClaimPaymentCommand(Guid.NewGuid(), payerId, "1234567890", 1_000m, "trace-outage-1"),
            CancellationToken.None);

        Assert.Equal(SagaState.Compensated, first.State);
        Assert.False(first.IsTransient);
        Assert.Equal(10_000m, store.GetBalance(payerId));

        // Second claim hits the open circuit and must fail fast with compensation.
        var second = await orchestrator.ExecuteAsync(
            new ClaimPaymentCommand(Guid.NewGuid(), payerId, "1234567890", 1_000m, "trace-outage-2"),
            CancellationToken.None);

        Assert.Equal(SagaState.Compensated, second.State);
        Assert.True(second.IsTransient);
        Assert.Equal(10_000m, store.GetBalance(payerId));
        Assert.True(handler.CallCount >= 4);
    }

    [Fact]
    public async Task ConcurrentDuplicateClaimIds_ShouldCoalesceIntoSingleClearinghouseExecution()
    {
        var store = new InMemoryPaymentLedgerStore();
        var payerId = Guid.NewGuid();
        store.SeedPayerBalance(payerId, 100_000m);

        var clearinghouse = new DelayingCountingClearinghouse();
        var orchestrator = new ClaimPaymentSagaOrchestrator(
            store,
            clearinghouse,
            NullLogger<ClaimPaymentSagaOrchestrator>.Instance);

        var claimId = Guid.NewGuid();
        var tasks = Enumerable.Range(0, 50).Select(_ => orchestrator.ExecuteAsync(
            new ClaimPaymentCommand(claimId, payerId, "1234567890", 125m, "trace-coalesce"),
            CancellationToken.None));

        var results = await Task.WhenAll(tasks);

        Assert.Equal(1, clearinghouse.CallCount);
        Assert.All(results, r => Assert.Equal(SagaState.Settled, r.State));
        Assert.Single(results.Select(r => r.SagaId).Distinct());
        Assert.Equal(99_875m, store.GetBalance(payerId));
    }

    private sealed class AlwaysUnavailableHandler : HttpMessageHandler
    {
        private int _callCount;

        public int CallCount => Volatile.Read(ref _callCount);

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref _callCount);
            return Task.FromResult(new HttpResponseMessage(HttpStatusCode.ServiceUnavailable));
        }
    }

    private sealed class DelayingCountingClearinghouse : IClearinghouseGateway
    {
        private int _callCount;

        public int CallCount => Volatile.Read(ref _callCount);

        public async Task<bool> SettleAchAsync(Guid sagaId, string providerNpi, decimal amount, CancellationToken ct)
        {
            Interlocked.Increment(ref _callCount);
            await Task.Delay(50, ct);
            return true;
        }
    }
}
