-- Healthcare Provider Claim Payments: Atomic Reservation + Transactional Outbox
CREATE TABLE dbo.PayerAccounts (
    PayerId UNIQUEIDENTIFIER PRIMARY KEY,
    OrganizationName NVARCHAR(150) NOT NULL,
    AvailableBalance DECIMAL(18, 2) NOT NULL,
    ReservedBalance DECIMAL(18, 2) NOT NULL DEFAULT 0.00,
    RowVer ROWVERSION NOT NULL,
    CONSTRAINT CK_Payer_PositiveFunds CHECK (AvailableBalance >= 0 AND ReservedBalance >= 0)
);

CREATE TABLE dbo.ClaimPaymentSagas (
    SagaId UNIQUEIDENTIFIER PRIMARY KEY,
    ClaimId UNIQUEIDENTIFIER NOT NULL UNIQUE, -- Client-generated Idempotency Key
    PayerId UNIQUEIDENTIFIER NOT NULL REFERENCES dbo.PayerAccounts(PayerId),
    ProviderNpi VARCHAR(10) NOT NULL,
    Amount DECIMAL(18, 2) NOT NULL,
    Status TINYINT NOT NULL, -- 1=Reserved, 2=Settled, 3=Compensated(RolledBack)
    TraceId VARCHAR(64) NOT NULL,
    CreatedAt DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME()
);

CREATE TABLE dbo.OutboxMessages (
    MessageId UNIQUEIDENTIFIER PRIMARY KEY,
    EventType VARCHAR(100) NOT NULL,
    AggregateId UNIQUEIDENTIFIER NOT NULL,
    Payload NVARCHAR(MAX) NOT NULL,
    TraceId VARCHAR(64) NOT NULL,
    CreatedAt DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
    ProcessedAt DATETIME2(3) NULL
);

-- Filtered index keeps outbox polling sub-millisecond even with millions of historical rows
CREATE INDEX IX_Outbox_Unprocessed ON dbo.OutboxMessages (CreatedAt)
INCLUDE (MessageId, EventType, AggregateId, Payload, TraceId)
WHERE ProcessedAt IS NULL;
GO

CREATE OR ALTER PROCEDURE dbo.usp_ReserveClaimFundsWithOutbox
    @SagaId UNIQUEIDENTIFIER,
    @ClaimId UNIQUEIDENTIFIER,
    @PayerId UNIQUEIDENTIFIER,
    @ProviderNpi VARCHAR(10),
    @Amount DECIMAL(18, 2),
    @TraceId VARCHAR(64)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    -- Idempotency Gate: Safe replay if network retry sends the same ClaimId
    IF EXISTS (SELECT 1 FROM dbo.ClaimPaymentSagas WHERE ClaimId = @ClaimId)
    BEGIN
        SELECT SagaId, Status FROM dbo.ClaimPaymentSagas WHERE ClaimId = @ClaimId;
        RETURN;
    END

    BEGIN TRANSACTION;

    -- Atomic Balance Check & Reservation (prevents SELECT + UPDATE race conditions & deadlocks)
    UPDATE dbo.PayerAccounts
    SET AvailableBalance = AvailableBalance - @Amount,
        ReservedBalance = ReservedBalance + @Amount
    WHERE PayerId = @PayerId
      AND AvailableBalance >= @Amount;

    IF @@ROWCOUNT = 0
    BEGIN
        ROLLBACK TRANSACTION;
        THROW 50001, 'Insufficient payer liquidity or invalid PayerId.', 1;
    END

    INSERT INTO dbo.ClaimPaymentSagas (SagaId, ClaimId, PayerId, ProviderNpi, Amount, Status, TraceId)
    VALUES (@SagaId, @ClaimId, @PayerId, @ProviderNpi, @Amount, 1, @TraceId);

    -- Write integration event in the SAME ACID transaction (prevents dual-write data loss)
    DECLARE @Payload NVARCHAR(MAX) = (
        SELECT @SagaId AS sagaId, @ClaimId AS claimId, @PayerId AS payerId,
               @ProviderNpi AS providerNpi, @Amount AS amount, @TraceId AS traceId
        FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
    );

    INSERT INTO dbo.OutboxMessages (MessageId, EventType, AggregateId, Payload, TraceId)
    VALUES (NEWID(), 'ClaimFundsReserved.v1', @ClaimId, @Payload, @TraceId);

    COMMIT TRANSACTION;
    SELECT @SagaId AS SagaId, 1 AS Status;
END;
GO

-- ---------------------------------------------------------------------------
-- Least-privilege RBAC for the application login.
-- The app role may only EXECUTE the reservation procedure. Direct table access is
-- denied so a compromised application credential cannot read payer balances or
-- delete outbox messages (transactional-outbox tampering / data exfiltration).
-- Ownership chaining (procedure and tables share the same owner) keeps the
-- procedure working even though the role is denied on the underlying tables.
-- ---------------------------------------------------------------------------
IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = 'HealthcarePaymentsApp' AND type = 'R')
    EXEC('CREATE ROLE HealthcarePaymentsApp AUTHORIZATION dbo;');
GO

GRANT EXECUTE ON OBJECT::dbo.usp_ReserveClaimFundsWithOutbox TO HealthcarePaymentsApp;
GO

DENY SELECT, INSERT, UPDATE, DELETE ON OBJECT::dbo.PayerAccounts TO HealthcarePaymentsApp;
DENY SELECT, INSERT, UPDATE, DELETE ON OBJECT::dbo.ClaimPaymentSagas TO HealthcarePaymentsApp;
DENY SELECT, INSERT, UPDATE, DELETE ON OBJECT::dbo.OutboxMessages TO HealthcarePaymentsApp;
GO