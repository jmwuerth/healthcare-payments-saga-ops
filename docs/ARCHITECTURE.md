# System Architecture & Distributed Transaction Design

## 1. Why Saga + Transactional Outbox Over Two-Phase Commit (2PC)

In distributed financial architectures, Two-Phase Commit (2PC) couples service availability and holds physical database row/page locks across slow network calls:
* **In-Doubt Locks:** If a transaction coordinator crashes after Phase 1 (`Prepare`), SQL Server rows remain locked until manual recovery, halting concurrent settlements for that payer.
* **Availability Coupling:** Every participant (Ledger DB, Compliance Engine, External ACH Clearinghouse) must be online simultaneously.

### Our Solution: Semantic Locking + Local ACID Outbox
Instead of holding physical locks over HTTP, we split the write path into a sub-millisecond local ACID transaction followed by asynchronous orchestration with deterministic compensation:

```mermaid
sequenceDiagram
    autonumber
    actor Client as Angular UI / Batch Stream
    participant API as Payments.Api (Minimal API)
    participant Guard as PaymentComplianceGuardrail
    participant DB as SQL Server (Ledger + Outbox)
    participant ACH as Clearinghouse Gateway

    Client->>API: POST /api/claims/settle (ClaimId Idempotency Key)
    API->>DB: EXEC usp_ReserveClaimFundsWithOutbox
    Note over DB: Single ACID Tx:<br/>1. Check ClaimId idempotency<br/>2. Move Available -> Reserved<br/>3. Insert OutboxMessages row
    DB-->>API: Reserved (SagaId, Status=1)

    API->>Guard: Validate(cmd) [Zero-Alloc]
    alt Compliance Violation
        Guard-->>API: Rejected (e.g., > \$250k limit)
        API->>DB: CompensateReservationAsync()
        API-->>Client: 409 Conflict (Compensated)
    else Compliant
        API->>ACH: SettleAchAsync(SagaId, NPI, Amount)
        alt Clearinghouse Rejects
            ACH-->>API: False
            API->>DB: CompensateReservationAsync()
            API-->>Client: 409 Conflict (Compensated)
        else Clearinghouse Settles
            ACH-->>API: True
            API->>DB: MarkSettledAsync(SagaId, Status=2)
            API-->>Client: 200 OK (Settled + TraceId)
        end
    end
```

---

## 2. Non-Blocking Ingestion Pipeline (`Pipelines` $\rightarrow$ `Channels`)

To ingest large batch claim files (NDJSON/EDI) without exhausting thread-pool threads or triggering Gen 2 Garbage Collection pauses:

```mermaid
flowchart LR
    A[HTTP Request Body Stream] -->|FillPipeAsync| B[System.IO.Pipelines<br/>64KB High / 32KB Low Watermark]
    B -->|ReadOnlySpan / SequenceReader| C[Utf8JsonReader<br/>Zero-Alloc Stack Parsing]
    C -->|Valid Frame| D[Bounded Channel&lt;T&gt;<br/>Capacity: 10,000 / Wait Mode]
    C -->|Malformed JSON| E[Poison-Frame Quarantine<br/>Structured Warning Log]
    D -->|ReadAllAsync| F[ClaimSagaConsumerWorker<br/>BackgroundService]
```

1. **TCP-Level Backpressure:** `PipeOptions` pauses `FillPipeAsync` at `65,536` bytes and resumes at `32,768` bytes if downstream processing slows down.
2. **Zero-Allocation Parsing:** `TryReadLineHybrid` slices contiguous memory segments using `ReadOnlySpan<byte>` (falling back to `SequenceReader<byte>` across multi-segment boundaries), and `Utf8JsonReader` deserializes fields directly from UTF-8 bytes without allocating intermediate `string` payloads.
3. **Consumer Backpressure:** `Channel.CreateBounded<ClaimPaymentCommand>(10_000)` with `BoundedChannelFullMode.Wait` asynchronously suspends ingestion tasks rather than dropping messages or buffering unboundedly in RAM.

---

## 3. Dual-Control Engineering Governance

| Control Layer | Implementation | Purpose |
| :--- | :--- | :--- |
| **AI Guardrails** | `AGENTS.md` | Constrains OpenCode and Copilot to zero-allocation hot paths, Saga+Outbox patterns, and OpenTelemetry `ActivitySource` tracing. |
| **Segregation of Duties** | `.github/CODEOWNERS` | Requires `@jmwuerth` approval for changes to `/sql/`, `/Guardrails/`, `/AGENTS.md`, or `/.github/workflows/`. |
| **Automated CI Gates** | `.github/workflows/ci-cd.yml` | Enforces `dotnet build -warnaserror` and blocks PR merges if concurrency, compensation, or `0-byte` GC SLA tests fail. |

---

## 4. Template Scope vs. Production Hardening Roadmap

This repository is structured as a lightweight, self-contained reference template (requiring no external database, broker, or Docker daemon to build and run) for local concurrency, resilience, and memory-SLA testing.
* **Current Reference Implementation:** Uses an in-memory ledger and deterministic clearinghouse simulator alongside `Microsoft.Extensions.Http.Resilience` (jittered retries, circuit breaker, timeouts), edge rate limiting, single-flight coalescing, reference T-SQL DDL (`sql/001_init_ledger_and_outbox.sql`), and non-blocking `System.IO.Pipelines` ingestion.
* **Production Hardening Roadmap:** Details future enhancements including lease-based Outbox Relay workers (`UPDLOCK, READPAST, ROWLOCK`), durable batch ingestion with poison-frame quarantine, and `Testcontainers.MsSql` / OpenTelemetry OTLP integration.

### Production-Ready Target Architecture (End-to-End Blueprint)

The diagram below illustrates the full production target state—combining our current **supply-chain security**, **browser CSP**, **edge rate limiting**, **single-flight coalescing**, and **Polly v8 circuit breaker** controls with the **lease-based T-SQL Outbox Relay (`UPDLOCK, READPAST, ROWLOCK`)** and **downstream consumer deduplication**:

```mermaid
flowchart TD
    subgraph GOV["0. Supply-Chain & Compiler Governance Gate (Build / CI / GitHub)"]
        direction LR
        CPM["Directory.Packages.props + packages.lock.json<br/>(--locked-mode SHA-512 verification)"]
        ROSLYN["Directory.Build.props<br/>(-warnaserror + SecurityCodeScan + SonarAnalyzer)"]
        OWNERS[".github/CODEOWNERS<br/>(Dual-Control on SQL, Guardrails & Props)"]
    end

    subgraph INGRESS["1. Hardened Client, Edge Load Shedding & Local ACID Boundary"]
        direction TB
        CLIENT["Angular UI (Strict CSP + exhaustMap)<br/>or Batch NDJSON Stream"]
        RL["Edge Token-Bucket Rate Limiter (PaymentIngressPolicy)<br/>Sheds bursts: 429 Too Many Requests + Jittered Retry-After (1-5s)"]
        COALESCE["Payments.Api Pod<br/>1. Single-Flight Coalescing: ConcurrentDictionary&lt;ClaimId, Lazy&lt;Task&gt;&gt;<br/>2. Zero-Alloc Guardrail: &lt;= \$250k, 10-digit NPI (0-byte GC SLA)<br/>3. Least-Privilege EXECUTE-only SQL RBAC"]
        SQL["SQL Server (Primary OLTP — Single Local ACID Transaction)<br/>(a) ClaimPaymentSagas → UNIQUE(ClaimId) check (Settled → 200; Reserved → 409)<br/>(b) PayerAccounts → Semantic Lock: AvailableBalance - Amt, ReservedBalance + Amt<br/>(c) OutboxMessages → INSERT (MessageId, ClaimId, Payload, TraceId, ProcessedAt=NULL)"]

        CLIENT -->|"POST /api/claims/settle<br/>(ClaimId + W3C traceparent)"| RL
        RL --> COALESCE
        COALESCE -->|"EXEC usp_ReserveClaimFundsWithOutbox"| SQL
    end

    GOV -.->|"Enforces build, test & review gates"| INGRESS

    subgraph RELAY["2. Lock-Free Horizontal Outbox Relay & Resilient Egress (Scaled-Out Pods)"]
        direction TB
        PODA["OutboxRelayWorker (Pod A)<br/>PeriodicTimer (200ms)<br/>Claims Rows 1..50 WITH (UPDLOCK, READPAST, ROWLOCK)<br/>LockedUntilUtc = NOW + 30s | AttemptCount += 1"]
        PODB["OutboxRelayWorker (Pod B)<br/>PeriodicTimer (200ms)<br/>Skips Rows 1..50 immediately (zero wait!)<br/>Claims Rows 51..100 | LockedUntilUtc = NOW + 30s"]
        CHAN["Bounded Channel&lt;T&gt; (Cap: 500)<br/>FullMode.Wait Backpressure + W3C ActivityContext"]
        POLLY["Polly v8 Egress Guard (AchClearinghouseGuard)<br/>• Decorrelated Jitter Exponential Retries (UseJitter = true)<br/>• Circuit Breaker (FailureRatio = 0.5, 30s break) + 2s Timeout<br/>• Open Circuit → CompensateReservationAsync() + 503 Fast Fail"]
        SUCC["Publish Succeeds<br/>1. Broker ACK (Partition Key = ClaimId)<br/>2. Fencing-Token Completion:<br/>UPDATE OutboxMessages SET ProcessedAt = NOW<br/>WHERE MessageId = @Id AND LockedUntilUtc = @Lease"]
        CRASH["Pod A Crashes / GC Freeze<br/>1. ProcessedAt stays NULL; after 30s lease expires<br/>2. Pod B reclaims expired row & republishes<br/>3. Late Pod A UPDATE fails (0 rows affected — zombie blocked)<br/>4. AttemptCount &gt;= 5 → Dead-Letter / Alert"]

        PODA --> CHAN
        PODB --> CHAN
        CHAN --> POLLY
        POLLY --> SUCC
        POLLY -.-> CRASH
    end

    SQL -->|"Filtered Covering Index: IX_Outbox_ActivePoll<br/>WHERE ProcessedAt IS NULL AND AttemptCount &lt; 5"| PODA
    SQL -->|"Filtered Covering Index: IX_Outbox_ActivePoll"| PODB

    subgraph CONSUMER["3. Downstream Idempotent Consumers (At-Least-Once → Effectively-Once)"]
        direction LR
        BROKER["Kafka / IBM MQ<br/>(Key: ClaimId, Header: traceparent)"]
        INBOX["Clearinghouse / Settlement Consumer<br/>Single Local Consumer Tx:<br/>1. INSERT INTO Inbox (MessageId) (PK violation → ignore retry)<br/>2. Execute downstream settlement"]
        BROKER --> INBOX
    end

    SUCC --> BROKER
    CRASH --> BROKER
```
