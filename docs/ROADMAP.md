# Reference Template vs. Production-Ready Gap Analysis

This repository is structured as a **zero-dependency (.NET 10 BCL-only) reference template** for local concurrency, Saga compensation, and memory-SLA testing without requiring Docker, SQL Server, or external message brokers.

The comparison below shows how the current reference template differs from a **production-ready deployment**, aligned directly with **Section 4** of [`docs/ARCHITECTURE.md`](ARCHITECTURE.md).

---

## Reference Template vs. Production-Ready Comparison

| Capability | Current Reference Template (BCL-Only) | Production-Ready Target State |
| :--- | :--- | :--- |
| **Ledger & Outbox Persistence** | Uses `InMemoryPaymentLedgerStore` (`ConcurrentDictionary`) in `Payments.Api`. Reference T-SQL DDL and stored procedures (`sql/001_init_ledger_and_outbox.sql`) are included for schema validation but not wired to runtime execution. | Wired to **SQL Server / Azure SQL** via `Microsoft.Data.SqlClient`, executing atomic fund reservations (`AvailableBalance` → `ReservedBalance`) and `dbo.OutboxMessages` inserts inside a single local ACID transaction. |
| **Transactional Outbox Relay** | Outbox messages are recorded in memory during reservation with no background dispatcher publishing events to downstream consumers. | Dedicated **lease-based Outbox Relay worker** (`BackgroundService`) polling `dbo.OutboxMessages` with `WITH (UPDLOCK, READPAST, ROWLOCK)` to publish `ClaimFundsReserved.v1` events safely across scaled-out replicas without duplicate delivery. |
| **State-Aware Replay Short-Circuiting** | `TryReserveFundsAsync` returns the existing `SagaId` on `ClaimId` replay, but `ClaimPaymentSagaOrchestrator` still re-runs guardrails and invokes `SimulatedClearinghouseGateway` again. | **State-aware replay short-circuiting** inspects the persisted Saga state (`Reserved`, `Settled`, `Compensated`) on duplicate `ClaimId` requests and immediately returns the terminal outcome without re-invoking downstream ACH clearinghouse calls. |
| **Durable Batch Ingestion & Poison-Frame Quarantine** | `ClaimStreamIngestor` streams NDJSON over `System.IO.Pipelines` into a bounded `Channel<T>` (`10,000` capacity); malformed JSON frames throw out of `ReadPipeAsync` and return HTTP `500`. | Uses hybrid `ReadOnlySpan<byte>` / `SequenceReader<byte>` zero-allocation framing and routes malformed or schema-invalid NDJSON records with trace metadata to a **durable poison-frame quarantine / dead-letter store** while valid frames continue processing. |
| **ACH Clearinghouse Gateway** | `SimulatedClearinghouseGateway` runs in-process and deterministically rejects NPIs starting with `999`. | Typed HTTP/gRPC **ISO 20022 / NACHA ACH client** wrapped in `Microsoft.Extensions.Http.Resilience` (timeouts, jittered exponential retries, and circuit breakers) plus a **Stuck-Saga Reaper** for crash recovery. |
| **Integration Testing & Observability** | Fast in-memory `xUnit` concurrency, compensation, and `0-byte` GC allocation tests (`PaymentSagaAndPipelineTests.cs`), with no-op `ActivitySource` instances. | Automated **`Testcontainers.MsSql`** integration test suite validating real `UPDLOCK`/`READPAST` lock contention and SQL constraints, paired with **OpenTelemetry OTLP exporters** for distributed traces, metrics, and HIPAA/SOC2 audit logs. |
| **API Security & Multi-Tenancy** | Unauthenticated Minimal API endpoints (`Program.cs`) for local development and Angular proxy testing. | **OAuth 2.0 / OIDC JWT authentication**, scope-based authorization (`claims.settle`), payer-tenant isolation, mTLS for clearinghouse callbacks, and ASP.NET Core rate limiting. |

---

## Production Hardening Checklist

* [ ] **SQL Server & Outbox Relay:** Wire `sql/001_init_ledger_and_outbox.sql` to `Payments.Api` and implement a leased `UPDLOCK, READPAST` Outbox Relay `BackgroundService`.
* [ ] **State-Aware Replay Short-Circuiting:** Short-circuit already-`Settled` or `Compensated` `ClaimId` replays before invoking guardrails or the ACH clearinghouse.
* [ ] **Durable Poison-Frame Quarantine:** Implement hybrid `SequenceReader<byte>` framing in `ClaimStreamIngestor` and isolate malformed NDJSON frames into a dead-letter queue without failing valid batch records.
* [ ] **Testcontainers & OpenTelemetry OTLP:** Add `Testcontainers.MsSql` concurrency tests and configure OTLP trace/metric exporters in `Program.cs`.
* [ ] **Authentication & Rate Limiting:** Enforce JWT bearer auth, tenant scoping, and endpoint rate limits on `/api/claims/*`.
