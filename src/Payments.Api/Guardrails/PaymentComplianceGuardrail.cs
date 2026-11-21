using Payments.Api.Pipelines;

namespace Payments.Api.Guardrails;

public sealed record ComplianceAuditResult(bool IsCompliant, string? ViolationReason = null);

/// <summary>
/// Deterministic policy guardrail sitting between AI-assisted claim adjudication
/// and actual funds movement. Ensures non-deterministic LLM outputs never bypass
/// banking limits or NPI formatting rules.
/// </summary>
public static class PaymentComplianceGuardrail
{
    public const decimal MaxAutomatedSettlementLimit = 250_000.00m;

    public static ComplianceAuditResult Validate(ClaimPaymentCommand cmd)
    {
        if (cmd.Amount <= 0m)
            return new ComplianceAuditResult(false, "Payment amount must be strictly positive.");

        if (cmd.Amount > MaxAutomatedSettlementLimit)
            return new ComplianceAuditResult(false, $"Amount ${cmd.Amount} exceeds automated threshold; requires dual human sign-off.");

        if (string.IsNullOrWhiteSpace(cmd.ProviderNpi) || cmd.ProviderNpi.Length != 10 || !cmd.ProviderNpi.All(char.IsDigit))
            return new ComplianceAuditResult(false, "Invalid 10-digit Healthcare Provider NPI.");

        return new ComplianceAuditResult(true);
    }
}
