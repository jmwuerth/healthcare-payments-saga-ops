using System.Threading.Channels;
using Payments.Api.Pipelines;
using Payments.Api.Sagas;

namespace Payments.Api.Hosting;

/// <summary>
/// Drains the bounded channel and executes the saga per frame. The HTTP response for
/// <c>ingest-batch</c> has already returned 202, so outcomes are observed via telemetry.
/// </summary>
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
