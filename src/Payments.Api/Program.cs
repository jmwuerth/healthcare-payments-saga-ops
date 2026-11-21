using System.Threading.Channels;
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
builder.Services.AddSingleton<IClearinghouseGateway, SimulatedClearinghouseGateway>();
builder.Services.AddSingleton<ClaimPaymentSagaOrchestrator>();
builder.Services.AddHostedService<ClaimSagaConsumerWorker>();

var app = builder.Build();

app.MapPost("/api/claims/ingest-batch", async (HttpRequest request, ClaimStreamIngestor ingestor, CancellationToken ct) =>
{
    int count = await ingestor.IngestStreamAsync(request.Body, ct);
    return Results.Accepted(value: new { QueuedClaims = count });
});

app.MapPost("/api/claims/settle", async (ClaimPaymentCommand cmd, ClaimPaymentSagaOrchestrator orchestrator, CancellationToken ct) =>
{
    var result = await orchestrator.ExecuteAsync(cmd, ct);
    return result.State == SagaState.Settled
        ? Results.Ok(result)
        : Results.Conflict(result);
});

app.Run();

public sealed class SimulatedClearinghouseGateway : IClearinghouseGateway
{
    public Task<bool> SettleAchAsync(Guid sagaId, string providerNpi, decimal amount, CancellationToken ct)
        => Task.FromResult(!providerNpi.StartsWith("999")); // NPIs starting with 999 simulate ACH rejection
}

public sealed class ClaimSagaConsumerWorker(
    ChannelReader<ClaimPaymentCommand> reader,
    ClaimPaymentSagaOrchestrator orchestrator) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await foreach (var cmd in reader.ReadAllAsync(stoppingToken))
        {
            await orchestrator.ExecuteAsync(cmd, stoppingToken);
        }
    }
}
