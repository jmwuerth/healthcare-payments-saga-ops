using Payments.Api.Sagas;

namespace Payments.Api.Hosting;

/// <summary>
/// Deterministic in-process ACH simulator used for local runs and tests, and as the
/// fallback whenever no live clearinghouse endpoint is configured.
/// NPIs starting with 999 simulate an ACH rejection.
/// </summary>
public sealed class SimulatedClearinghouseGateway : IClearinghouseGateway
{
    public Task<bool> SettleAchAsync(Guid sagaId, string providerNpi, decimal amount, CancellationToken ct)
        => Task.FromResult(!providerNpi.StartsWith("999", StringComparison.Ordinal));
}
