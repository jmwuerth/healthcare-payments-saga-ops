# Future Enhancements & OpenCode Prompts

Copy and paste either prompt below into **OpenCode** (`opencode`) from the repository root to scaffold the next production capability while preserving `AGENTS.md` guardrails.

---

## 1. SQL Server / Testcontainers Integration Tests (`001_init_ledger_and_outbox.sql`)

```text
Follow AGENTS.md and wire up a real SQL Server integration test suite using Testcontainers.MsSql and Microsoft.Data.SqlClient:
1. Add Testcontainers.MsSql and Microsoft.Data.SqlClient to tests/Payments.Tests/Payments.Tests.csproj (keep -warnaserror clean).
2. Create tests/Payments.Tests/SqlLedgerOutboxIntegrationTests.cs using an IAsyncLifetime fixture that starts an MsSqlBuilder container, executes sql/001_init_ledger_and_outbox.sql to create the schema and stored procedures, and seeds a payer account.
3. Add integration tests verifying:
   - Atomic semantic lock (AvailableBalance -> ReservedBalance) and dbo.OutboxMessages insertion inside a single SQL transaction.
   - Concurrent duplicate ClaimId calls race safely against the UNIQUE constraint without double-deducting AvailableBalance or writing duplicate outbox messages.
   - Compensating release restores AvailableBalance cleanly when a Saga fails.
4. If Docker is unavailable in the local environment, skip the container test gracefully rather than failing fast unit test runs, and run dotnet test HealthcarePayments.sln --configuration Release -warnaserror to verify.
```

---

## 2. OpenTelemetry Console & OTLP Exporter Setup (`Program.cs`)

```text
Follow AGENTS.md and configure OpenTelemetry distributed tracing and metrics in src/Payments.Api/Program.cs:
1. Add OpenTelemetry.Extensions.Hosting, OpenTelemetry.Instrumentation.AspNetCore, OpenTelemetry.Instrumentation.Http, OpenTelemetry.Exporter.Console, and OpenTelemetry.Exporter.OpenTelemetryProtocol to src/Payments.Api/Payments.Api.csproj.
2. Register OpenTelemetry in Program.cs with ConfigureResource (service name "HealthcarePayments.Api"), subscribing to ClaimPaymentSagaOrchestrator.Telemetry.Name ("COMPANYNAME.HealthcarePayments.Saga") plus ASP.NET Core and HttpClient instrumentation.
3. Configure exporters so OTEL_EXPORTER_OTLP_ENDPOINT enables the OTLP exporter when set, while falling back to the Console exporter in Development (and avoiding noisy console spam during Testing environments so xUnit output stays clean).
4. Ensure incoming traceId / traceparent headers correlate with Activity.Current and update README.md with an example OTEL_EXPORTER_OTLP_ENDPOINT environment variable snippet.
5. Run dotnet build HealthcarePayments.sln --configuration Release -warnaserror and dotnet test HealthcarePayments.sln --configuration Release --no-build to verify.
```
