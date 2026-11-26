using System.Globalization;
using System.Security.Cryptography;
using System.Threading.Channels;
using System.Threading.RateLimiting;
using Microsoft.AspNetCore.RateLimiting;
using Payments.Api.Hosting;
using Payments.Api.Pipelines;
using Payments.Api.Sagas;

var builder = WebApplication.CreateBuilder(args);

// Bounded Channel provides backpressure when inbound payment bursts exceed DB write throughput
var channel = Channel.CreateBounded<ClaimPaymentCommand>(new BoundedChannelOptions(10_000)
{
    FullMode = BoundedChannelFullMode.Wait,
    SingleReader = false,
    SingleWriter = false
});

builder.Services.AddSingleton(channel.Writer);
builder.Services.AddSingleton(channel.Reader);
builder.Services.AddSingleton<ClaimStreamIngestor>();
builder.Services.AddSingleton<InMemoryPaymentLedgerStore>();
builder.Services.AddSingleton<IPaymentLedgerStore>(sp => sp.GetRequiredService<InMemoryPaymentLedgerStore>());
builder.Services.AddSingleton<SimulatedClearinghouseGateway>();

// ACH egress: jittered exponential retries, circuit breaker and a 2s per-attempt timeout.
// When Clearinghouse:BaseUrl is not configured the gateway delegates to the deterministic simulator.
builder.Services
    .AddHttpClient<IClearinghouseGateway, HttpClearinghouseGateway>(client =>
    {
        client.Timeout = Timeout.InfiniteTimeSpan; // per-attempt timeout is owned by the resilience pipeline
    })
    .AddResilienceHandler("AchClearinghouseGuard", AchResiliencePipeline.Configure);

builder.Services.AddSingleton<ClaimPaymentSagaOrchestrator>();
builder.Services.AddHostedService<ClaimSagaConsumerWorker>();

// Token-bucket edge limiter. QueueLimit = 0 rejects immediately instead of queueing a thundering herd.
builder.Services.AddRateLimiter(options =>
{
    options.RejectionStatusCode = StatusCodes.Status429TooManyRequests;
    options.AddTokenBucketLimiter("PaymentIngressPolicy", limiter =>
    {
        limiter.TokenLimit = 100;
        limiter.TokensPerPeriod = 50;
        limiter.ReplenishmentPeriod = TimeSpan.FromSeconds(1);
        limiter.AutoReplenishment = true;
        limiter.QueueLimit = 0;
        limiter.QueueProcessingOrder = QueueProcessingOrder.OldestFirst;
    });

    options.OnRejected = (context, _) =>
    {
        // Crypto RNG (SCS0005-safe) yields an inclusive 1-5 second Retry-After jitter.
        context.HttpContext.Response.Headers.RetryAfter =
            RandomNumberGenerator.GetInt32(1, 6).ToString(CultureInfo.InvariantCulture);
        return ValueTask.CompletedTask;
    };
});

var app = builder.Build();
app.UseRateLimiter();

var claims = app.MapGroup("/api/claims").RequireRateLimiting("PaymentIngressPolicy");

claims.MapPost("/ingest-batch", async (HttpRequest request, ClaimStreamIngestor ingestor, CancellationToken ct) =>
{
    int count = await ingestor.IngestStreamAsync(request.Body, ct);
    return Results.Accepted(value: new { QueuedClaims = count });
});

claims.MapPost("/settle", async (ClaimPaymentCommand cmd, ClaimPaymentSagaOrchestrator orchestrator, CancellationToken ct) =>
{
    var result = await orchestrator.ExecuteAsync(cmd, ct);
    if (result.State == SagaState.Settled) return Results.Ok(result);
    if (result.IsTransient) return Results.Json(result, statusCode: StatusCodes.Status503ServiceUnavailable);
    return Results.Conflict(result);
});

await app.RunAsync();
