using System.Net.Http.Json;
using Payments.Api.Sagas;

namespace Payments.Api.Hosting;

/// <summary>
/// HTTP egress for the external ACH clearinghouse, wrapped by the
/// "AchClearinghouseGuard" resilience handler (jittered retries, circuit breaker, 2s timeout).
/// When no <c>Clearinghouse:BaseUrl</c> is configured it delegates to the deterministic
/// in-process simulator so local runs and tests never depend on an external endpoint.
/// </summary>
public sealed class HttpClearinghouseGateway(
    HttpClient http,
    SimulatedClearinghouseGateway deterministicFallback,
    IConfiguration configuration) : IClearinghouseGateway
{
    private readonly string? _baseUrl = configuration["Clearinghouse:BaseUrl"];

    public Task<bool> SettleAchAsync(Guid sagaId, string providerNpi, decimal amount, CancellationToken ct)
    {
        if (string.IsNullOrWhiteSpace(_baseUrl))
        {
            return deterministicFallback.SettleAchAsync(sagaId, providerNpi, amount, ct);
        }

        return SettleOverHttpAsync(sagaId, providerNpi, amount, ct);
    }

    private async Task<bool> SettleOverHttpAsync(Guid sagaId, string providerNpi, decimal amount, CancellationToken ct)
    {
        using var response = await http.PostAsJsonAsync(
            "/ach/settlements",
            new { sagaId, providerNpi, amount },
            ct);
        return response.IsSuccessStatusCode;
    }
}
