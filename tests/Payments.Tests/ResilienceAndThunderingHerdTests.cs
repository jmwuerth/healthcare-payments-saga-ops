using System.Net;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Payments.Api.Hosting;
using Payments.Api.Pipelines;
using Payments.Api.Sagas;
using System.Net.Http.Json; 
using Microsoft.AspNetCore.Mvc.Testing;
using System.Globalization;
using Xunit;

namespace Payments.Tests;

public sealed class ResilienceAndThunderingHerdTests
{
    [Fact]
    public async Task PaymentIngressRateLimiter_ShouldShedBurstWith429AndJitteredRetryAfter()
    {
        await using var factory = new WebApplicationFactory<Program>();
        using var client = factory.CreateClient();

        // PaymentIngressPolicy has TokenLimit = 100 and QueueLimit = 0
        // firing 120 concurrent requests guarantees immediate 429 load shedding.
        var requests = Enumerable.Range(0, 120).Select(_ =>
            client.PostAsJsonAsync("/api/claims/settle", new
            {
                claimId = Guid.NewGuid(),
                payerId = Guid.NewGuid(),
                providerNpi = "1234567890",
                amount = 100m,
                traceId = "trace-rl"
            }));

        var responses = await Task.WhenAll(requests);
        var throttled = responses.Where(r => r.StatusCode == HttpStatusCode.TooManyRequests).ToList();

        Assert.NotEmpty(throttled);
        Assert.All(throttled, res =>
        {
            Assert.True(res.Headers.TryGetValues("Retry-After", out var values));
            int retryAfter = int.Parse(values.Single(), CultureInfo.InvariantCulture);
            Assert.InRange(retryAfter, 1, 5); // Proves randomized jitter (1–5s)
        });
    }

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
