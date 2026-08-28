-- Migración idempotente para una base FinanData v1.0 ya creada.
-- Ruta recomendada: ejecutar python scripts/test_azure_connection.py.
-- Este archivo queda como evidencia y alternativa manual en SSMS.

IF OBJECT_ID(N'finandata.fact_transactions', N'U') IS NULL
    THROW 51000, 'Primero debe existir finandata.fact_transactions.', 1;

IF COL_LENGTH(N'finandata.fact_transactions', N'record_hash') IS NULL
    ALTER TABLE finandata.fact_transactions ADD record_hash CHAR(64) NULL;
IF COL_LENGTH(N'finandata.fact_transactions', N'created_at') IS NULL
    ALTER TABLE finandata.fact_transactions ADD created_at DATETIME2(3) NULL;
IF COL_LENGTH(N'finandata.fact_transactions', N'last_modified_at') IS NULL
    ALTER TABLE finandata.fact_transactions ADD last_modified_at DATETIME2(3) NULL;
IF COL_LENGTH(N'finandata.fact_transactions', N'created_batch_id') IS NULL
    ALTER TABLE finandata.fact_transactions ADD created_batch_id VARCHAR(80) NULL;
IF COL_LENGTH(N'finandata.fact_transactions', N'last_modified_batch_id') IS NULL
    ALTER TABLE finandata.fact_transactions ADD last_modified_batch_id VARCHAR(80) NULL;
GO

UPDATE finandata.fact_transactions
SET created_at = COALESCE(created_at, processed_at),
    last_modified_at = COALESCE(last_modified_at, processed_at),
    created_batch_id = COALESCE(created_batch_id, batch_id),
    last_modified_batch_id = COALESCE(last_modified_batch_id, batch_id),
    record_hash = COALESCE(
        record_hash,
        LOWER(CONVERT(VARCHAR(64), HASHBYTES('SHA2_256', CONCAT_WS('|',
            transaction_id, account_from, account_to, amount, currency,
            transaction_ts, branch_id, channel, commission, risk_score,
            risk_level, customer_document_hash, card_masked,
            iban_from_masked, iban_to_masked, debit_amount, credit_amount,
            source_file, source_row
        )), 2))
    )
WHERE record_hash IS NULL OR created_at IS NULL
   OR last_modified_at IS NULL OR created_batch_id IS NULL
   OR last_modified_batch_id IS NULL;
GO

IF EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'finandata.fact_transactions')
      AND name = N'record_hash' AND is_nullable = 1
) ALTER TABLE finandata.fact_transactions
    ALTER COLUMN record_hash CHAR(64) NOT NULL;
IF EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'finandata.fact_transactions')
      AND name = N'created_at' AND is_nullable = 1
) ALTER TABLE finandata.fact_transactions
    ALTER COLUMN created_at DATETIME2(3) NOT NULL;
IF EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'finandata.fact_transactions')
      AND name = N'last_modified_at' AND is_nullable = 1
) ALTER TABLE finandata.fact_transactions
    ALTER COLUMN last_modified_at DATETIME2(3) NOT NULL;
IF EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'finandata.fact_transactions')
      AND name = N'created_batch_id' AND is_nullable = 1
) ALTER TABLE finandata.fact_transactions
    ALTER COLUMN created_batch_id VARCHAR(80) NOT NULL;
IF EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'finandata.fact_transactions')
      AND name = N'last_modified_batch_id' AND is_nullable = 1
) ALTER TABLE finandata.fact_transactions
    ALTER COLUMN last_modified_batch_id VARCHAR(80) NOT NULL;

IF COL_LENGTH(N'finandata.etl_batches', N'created_at') IS NULL
    ALTER TABLE finandata.etl_batches ADD created_at DATETIME2(3) NULL;
GO

UPDATE finandata.etl_batches
SET created_at = COALESCE(created_at, updated_at)
WHERE created_at IS NULL;
GO

IF EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'finandata.etl_batches')
      AND name = N'created_at' AND is_nullable = 1
) ALTER TABLE finandata.etl_batches
    ALTER COLUMN created_at DATETIME2(3) NOT NULL;
GO

IF OBJECT_ID(N'finandata.transaction_audit_history', N'U') IS NULL
CREATE TABLE finandata.transaction_audit_history (
    audit_id VARCHAR(80) NOT NULL PRIMARY KEY,
    transaction_id VARCHAR(40) NOT NULL,
    event_type VARCHAR(20) NOT NULL
        CHECK (event_type IN ('INSERT', 'UPDATE', 'REPROCESSED')),
    batch_id VARCHAR(80) NOT NULL,
    flow_run_id VARCHAR(100) NULL,
    pipeline_version VARCHAR(80) NULL,
    previous_record_hash CHAR(64) NULL,
    new_record_hash CHAR(64) NOT NULL,
    changed_columns_json NVARCHAR(MAX) NOT NULL,
    before_record_json NVARCHAR(MAX) NULL,
    after_record_json NVARCHAR(MAX) NOT NULL,
    source_file VARCHAR(260) NOT NULL,
    source_row INT NOT NULL,
    changed_at DATETIME2(3) NOT NULL
);
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'finandata.transaction_audit_history')
      AND name = N'IX_transaction_audit_history_transaction'
) CREATE INDEX IX_transaction_audit_history_transaction
    ON finandata.transaction_audit_history (transaction_id, changed_at DESC);

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'finandata.transaction_audit_history')
      AND name = N'IX_transaction_audit_history_batch'
) CREATE INDEX IX_transaction_audit_history_batch
    ON finandata.transaction_audit_history (batch_id, event_type);
GO

SELECT N'Auditoría FinanData v1.1 migrada correctamente.' AS result;
