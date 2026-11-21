# COMPANYNAME Healthcare Payments — Agent Notes

## Ground truth

- `main` has **zero commits**; every file is untracked. Stage explicit paths (`.gitignore` already covers `bin/`/`obj/`).
- `README.md` and `docs/ARCHITECTURE.md` describe the *intended* design and overstate what exists. Trust the code. Known doc/code gaps:
  - No poison-frame quarantine: a malformed NDJSON line throws out of `ClaimStreamIngestor.ReadPipeAsync` and the request returns **500** (verified via `POST /api/claims/ingest-batch`).
  - No `SequenceReader`/hybrid frame reader; framing is a plain `ReadOnlySequence.PositionOf('\n')`.
  - Zero-allocation is a target, not a fact: `Utf8JsonReader.GetString()` allocates for `providerNpi`.

## Commands

.NET 10 SDK only (`net10.0`, installed 10.0.100). No `global.json`, `Directory.Build.props`, or `.editorconfig`; warnings are errors **only** via the flag.

```bash
dotnet build HealthcarePayments.sln --configuration Release -warnaserror   # CI parity
dotnet test  HealthcarePayments.sln --configuration Release                # 4 tests (3 real + empty UnitTest1)
dotnet test  tests/Payments.Tests/Payments.Tests.csproj \
  --filter FullyQualifiedName~PaymentSagaAndPipeline                       # the 3 saga/pipeline tests
dotnet run --project src/Payments.Api                                      # http://localhost:5065
```

- CI runs `build -warnaserror` then `test --no-build`, so compile errors in `Payments.Api` (which the tests reference) block every gate.
- Only `src/Payments.Api` and `tests/Payments.Tests` are in the solution; a new project is invisible to CI until added to `HealthcarePayments.sln`.
- Filter by class name (`PaymentSagaAndPipeline`); `~ClaimPaymentSaga` matches nothing.

## Package constraint (verified)

`Payments.Api.csproj` deliberately has **no** `PackageReference`s — it is BCL-only. Do not add `Microsoft.AspNetCore.OpenApi` **11.x**: its Roslyn 5.11 generator fails on SDK 10.0.100 with `CS9057`. If OpenAPI is genuinely needed, the verified-clean pair is `Microsoft.AspNetCore.OpenApi` **10.0.15** + `Microsoft.OpenApi` **2.13.1** (10.0.15 requires `[2.13.0, 3.0.0)`; don't fall back to 2.0.0 — that re-triggers `NU1903`).

## Runtime wiring

- Endpoints that exist: `POST /api/claims/settle` and `POST /api/claims/ingest-batch`. `/weatherforecast` is gone — `src/Payments.Api/Payments.Api.http` is a stale scratch file.
- **Nothing seeds payer balances.** `InMemoryPaymentLedgerStore` starts empty and `SeedPayerBalance` is only called from tests, so a fresh `dotnet run` returns **409 "Insufficient payer liquidity"** for every claim. Exercise the success path in tests, not over HTTP.
- `409` bodies are the full `SagaExecutionRecord` with `state: 3` (Compensated) — this includes pure rejections where no funds ever moved and `sagaId` is all zeros. `state` is the numeric enum (`1` Reserved, `2` Settled, `3` Compensated).
- `SimulatedClearinghouseGateway` (in `Program.cs`) rejects any NPI starting with `999` — the fastest way to force a compensation path.
- `ingest-batch` returns `202` with the count of *parsed* frames; the `ClaimSagaConsumerWorker` settles them asynchronously afterward.
- `sql/001_init_ledger_and_outbox.sql` (SQL Server: `dbo`, `GO`, `THROW`) is never executed, migrated, or referenced. The in-memory store is the only ledger.
- `frontend/` has no `package.json` or toolchain — don't run npm/node or invent frontend scripts.

## Invariants to preserve

- Idempotency is keyed on `ClaimId`: `TryReserveFundsAsync` returns the existing `SagaId` on replay (the SQL proc uses the same gate). Note the replay path still re-runs the guardrail **and** calls the clearinghouse again, so a replayed settled claim triggers a second ACH settlement in the in-memory store.
- Guardrails run **after** reservation, so violations must compensate rather than reject early. Keep that ordering unless you also rework `ExecuteAsync` and the concurrency test.
- `CompensateReservationAsync` only restores funds while state is `Reserved`; every post-reservation failure path (guardrail or clearinghouse) must call it.
- `PaymentComplianceGuardrail`: amount `> 0` and `<= 250_000m`, NPI exactly 10 digits. CODEOWNER-gated.
- `ActivitySource`s are `COMPANYNAME.HealthcarePayments.Ingestor` and `.Saga`, with no listener/exporter registered — activities are no-ops, so `traceId` falls back to a random hex string. Passing tests do not prove trace propagation.

## Review gates

`.github/CODEOWNERS` requires @jmwuerth sign-off on `AGENTS.md`, `src/Payments.Api/Guardrails/`, `.github/workflows/`, and `sql/` — expect latency on guardrail, CI, and ledger-schema changes.
