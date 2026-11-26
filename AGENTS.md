# COMPANYNAME Healthcare Payments — Agent Notes

## Ground truth

- `main` has **zero commits**; every file is untracked. Stage explicit paths (`.gitignore` already covers `bin/`/`obj/`).
- `README.md` and `docs/ARCHITECTURE.md` describe the *intended* design and overstate what exists. Trust the code. Known doc/code gaps:
  - No poison-frame quarantine: a malformed NDJSON line throws out of `ClaimStreamIngestor.ReadPipeAsync` and the request returns **500** (verified via `POST /api/claims/ingest-batch`).
  - No `SequenceReader`/hybrid frame reader; framing is a plain `ReadOnlySequence.PositionOf('\n')`.
  - Zero-allocation is a target, not a fact: `Utf8JsonReader.GetString()` allocates for `providerNpi`.

## Commands

.NET 10 SDK only (`net10.0`, installed 10.0.100). No `global.json` or `.editorconfig`. The root `Directory.Build.props` sets `TreatWarningsAsErrors`, `Nullable`, `ImplicitUsings`, `EnableNETAnalyzers`, `AnalysisLevel=latest-recommended` and two private analyzers for **every** project, so warnings fail the build even without the flag. Package versions are centralized in `Directory.Packages.props`; `packages.lock.json` files are committed and CI restores with `--locked-mode`.

```bash
dotnet restore HealthcarePayments.sln --locked-mode                        # CI parity; fails on stale lockfiles
dotnet build   HealthcarePayments.sln --configuration Release --no-restore -warnaserror
dotnet test    HealthcarePayments.sln --configuration Release --no-build   # 8 tests
dotnet test    tests/Payments.Tests/Payments.Tests.csproj \
  --filter FullyQualifiedName~PaymentSagaAndPipeline                       # 6 saga/pipeline tests
dotnet test    tests/Payments.Tests/Payments.Tests.csproj \
  --filter FullyQualifiedName~ResilienceAndThunderingHerd                  # 2 resilience tests
dotnet run --project src/Payments.Api                                      # http://localhost:5065
```

- CI runs `restore --locked-mode`, then `build --no-restore -warnaserror`, then `test --no-build`, so compile errors in `Payments.Api` (which the tests reference) block every gate.
- Adding, removing, or upgrading a package requires `dotnet restore` to regenerate both lockfiles, or `--locked-mode` fails the build.
- Only `src/Payments.Api` and `tests/Payments.Tests` are in the solution; a new project is invisible to CI until added to `HealthcarePayments.sln`.
- Filter by class name (`PaymentSagaAndPipeline`); `~ClaimPaymentSaga` matches nothing.

## Package constraint (verified)

`Payments.Api` is **no longer BCL-only**: it references `Microsoft.Extensions.Http.Resilience` **10.11.1** (latest 10.x; 11.0.x targets net11) for the ACH egress pipeline. Every version lives in `Directory.Packages.props` — never put `Version="..."` on a `PackageReference`. Verified analyzer pins: `SecurityCodeScan.VS2019` **5.6.7** and `SonarAnalyzer.CSharp` **10.37.0.15768** both load cleanly on SDK 10.0.100 (no `CS9057`/`CS8032`), and SecurityCodeScan is live (it flags `Random.Shared` via `SCS0005`).

Do not add `Microsoft.AspNetCore.OpenApi` **11.x**: its Roslyn 5.11 generator fails on SDK 10.0.100 with `CS9057`. If OpenAPI is genuinely needed, the verified-clean pair is `Microsoft.AspNetCore.OpenApi` **10.0.15** + `Microsoft.OpenApi` **2.13.1** (10.0.15 requires `[2.13.0, 3.0.0)`; don't fall back to 2.0.0 — that re-triggers `NU1903`).

## Runtime wiring

- Endpoints that exist: `POST /api/claims/settle` and `POST /api/claims/ingest-batch`. `/weatherforecast` is gone — `src/Payments.Api/Payments.Api.http` is a stale scratch file.
- **Nothing seeds payer balances.** `InMemoryPaymentLedgerStore` starts empty and `SeedPayerBalance` is only called from tests, so a fresh `dotnet run` returns **409 "Insufficient payer liquidity"** for every claim. Exercise the success path in tests, not over HTTP.
- `409` bodies are the full `SagaExecutionRecord` with `state: 3` (Compensated) — this includes pure rejections where no funds ever moved and `sagaId` is all zeros. `state` is the numeric enum (`1` Reserved, `2` Settled, `3` Compensated).
- Both endpoints sit behind the `PaymentIngressPolicy` token bucket (100 burst / 50 per second, `QueueLimit = 0`), so an overload returns **429** with a randomized `Retry-After: 1..5`. The bucket is global, not per-IP.
- A tripped ACH circuit breaker returns **503** with the same `SagaExecutionRecord` shape plus `"isTransient": true`; funds are already compensated when that response is produced.
- `SimulatedClearinghouseGateway` (in `Hosting/`, no longer in `Program.cs`) rejects any NPI starting with `999` — the fastest way to force a compensation path. `HttpClearinghouseGateway` delegates to it whenever `Clearinghouse:BaseUrl` is unset, so local runs never touch the network.
- `ingest-batch` returns `202` with the count of *parsed* frames; the `ClaimSagaConsumerWorker` settles them asynchronously afterward.
- `sql/001_init_ledger_and_outbox.sql` (SQL Server: `dbo`, `GO`, `THROW`) is never executed, migrated, or referenced. The in-memory store is the only ledger.
- `frontend/` is a real Angular 20 workspace with a local mock API. From `frontend/`: `npm install`, then `npm run start:mock` (runs `mock-api.mjs` + `ng serve --proxy-config proxy.conf.json`). Verify AOT with `npx ng build`; use `npm run mock -- --409` or the `X-Mock-Status: 409` header to exercise compensation. `src/index.html` carries a strict CSP meta tag: `script-src 'self'` blocks inline scripts, so `ng serve` live-reload (Vite websocket) is blocked while `npx ng build` is unaffected. `frame-ancestors` is inert in a meta tag and only works as an HTTP header.

## Invariants to preserve

- Idempotency is keyed on `ClaimId`: `TryReserveFundsAsync` returns the existing `SagaId` on replay (the SQL proc uses the same gate). A `Settled` replay short-circuits inside `ExecuteAsync` and returns the cached `SagaExecutionRecord` — no second guardrail run and no second ACH settlement (asserted by `SettledReplay_ShouldReturnExistingRecordWithoutSecondClearinghouseCall`).
- Single-flight coalescing (`_inFlight`, `ConcurrentDictionary<Guid, Lazy<Task<SagaExecutionRecord>>>`) collapses concurrent duplicates for one `ClaimId` into a single execution. Eviction happens only after the shared task completes and only for that exact entry; a cancelled leader's token propagates to every waiter. Keep the ledger's `Reserved` rejection even though HTTP-level duplicates now rarely reach it.
- `CompensateReservationAsync` only restores funds while state is `Reserved`; every post-reservation failure path (guardrail, clearinghouse `false`, or `BrokenCircuitException`) must call it. Circuit-open outcomes set `IsTransient: true`, which is what maps to **503** instead of **409**.
- Guardrails run **after** reservation, so violations must compensate rather than reject early. Keep that ordering unless you also rework `ExecuteAsync` and the concurrency test.
- `PaymentComplianceGuardrail`: amount `> 0` and `<= 250_000m`, NPI exactly 10 digits. CODEOWNER-gated.
- `ActivitySource`s are `COMPANYNAME.HealthcarePayments.Ingestor` and `.Saga`, with no listener/exporter registered — activities are no-ops, so `traceId` falls back to a random hex string. Passing tests do not prove trace propagation.

## Rule 5 — Anti-Thundering-Herd & Supply-Chain Resilience

Non-negotiable gates; each is enforced by code plus a test or CI step, not by convention.

1. **Edge admission control.** `/api/claims` is gated by the `PaymentIngressPolicy` token bucket (`QueueLimit = 0`, `FullMode`-style immediate rejection). Never raise `QueueLimit` to "smooth" a spike: queueing is what turns a burst into a thundering herd. Rejections must stay `429` with a jittered `Retry-After` (1–5 s) so clients de-synchronize. Use `RandomNumberGenerator.GetInt32` — `Random.Shared` is rejected by `SCS0005`.
2. **Egress resilience.** The ACH client is registered through `AddHttpClient<IClearinghouseGateway, HttpClearinghouseGateway>()` + `.AddResilienceHandler("AchClearinghouseGuard", AchResiliencePipeline.Configure)`. Keep retries jittered (`UseJitter = true`), keep `FailureRatio = 0.5`, keep the 2 s per-attempt timeout, and keep `HttpClient.Timeout = InfiniteTimeSpan` so the pipeline owns timing. `MinimumThroughput = 4` is deliberate — the HTTP default of 100 would never trip on a low-volume ACH endpoint.
3. **Single-flight.** Concurrent retries for one `ClaimId` must collapse into one saga execution (`_inFlight` + `Lazy<Task<...>>`). Do not remove the coalescing layer without replacing the invariant test.
4. **Compensate on open circuit.** `BrokenCircuitException` is not a generic failure: compensate immediately, return `Compensated` with `IsTransient: true`, and let the HTTP boundary answer **503**. Never let a circuit-open outcome surface as **409**.
5. **Supply chain.** All versions live in `Directory.Packages.props` (`ManagePackageVersionsCentrally`, transitive pinning, lockfiles). `PackageReference` must not carry `Version="..."`. CI restores `--locked-mode`, so a dependency change that skips lockfile regeneration fails the build. Analyzers are pinned and private; do not "fix" a new analyzer finding by widening `NoWarn` without an inline justification comment.
6. **Blast radius.** Keep the SQL least-privilege block (`GRANT EXECUTE` only, `DENY` direct table access) and the frontend CSP meta tag intact. A new external origin must be added to `connect-src` explicitly — never relax to `default-src *`.

## Review gates

`.github/CODEOWNERS` requires @jmwuerth sign-off on `AGENTS.md`, `src/Payments.Api/Guardrails/`, `.github/workflows/`, `sql/`, `Directory.Build.props`, `Directory.Packages.props`, and `**/packages.lock.json` — expect latency on guardrail, CI, ledger-schema, and dependency changes.
