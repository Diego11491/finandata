-- Esquema de referencia. El pipeline también migra automáticamente las tablas.
IF SCHEMA_ID(N'finandata') IS NULL
    EXEC(N'CREATE SCHEMA finandata');
GO

IF OBJECT_ID(N'finandata.fact_transactions', N'U') IS NULL
CREATE TABLE finandata.fact_transactions (
    transaction_id VARCHAR(40) NOT NULL PRIMARY KEY,
    account_from VARCHAR(40) NOT NULL,
    account_to VARCHAR(40) NOT NULL,
    amount DECIMAL(18,2) NOT NULL,
    currency CHAR(3) NOT NULL,
    transaction_ts DATETIME2(3) NOT NULL,
    branch_id VARCHAR(20) NOT NULL,
    channel VARCHAR(20) NOT NULL,
    commission DECIMAL(18,2) NOT NULL,
    risk_score INT NOT NULL,
    risk_level VARCHAR(10) NOT NULL,
    customer_document_hash CHAR(64) NOT NULL,
    card_masked VARCHAR(30) NOT NULL,
    iban_from_masked VARCHAR(40) NOT NULL,
    iban_to_masked VARCHAR(40) NOT NULL,
    debit_amount DECIMAL(18,2) NOT NULL,
    credit_amount DECIMAL(18,2) NOT NULL,
    batch_id VARCHAR(80) NOT NULL,
    processed_at DATETIME2(3) NOT NULL,
    source_file VARCHAR(260) NOT NULL,
    source_row INT NOT NULL,
    record_hash CHAR(64) NOT NULL,
    created_at DATETIME2(3) NOT NULL,
    last_modified_at DATETIME2(3) NOT NULL,
    created_batch_id VARCHAR(80) NOT NULL,
    last_modified_batch_id VARCHAR(80) NOT NULL
);
GO

IF OBJECT_ID(N'finandata.etl_batches', N'U') IS NULL
CREATE TABLE finandata.etl_batches (
    batch_id VARCHAR(80) NOT NULL PRIMARY KEY,
    scenario VARCHAR(20) NOT NULL,
    flow_run_id VARCHAR(100) NULL,
    status VARCHAR(40) NOT NULL,
    total_records INT NOT NULL,
    accepted_records INT NOT NULL,
    rejected_records INT NOT NULL,
    rejection_rate DECIMAL(9,6) NOT NULL,
    inserted_records INT NOT NULL,
    updated_records INT NOT NULL,
    git_commit VARCHAR(80) NULL,
    detail_json NVARCHAR(MAX) NOT NULL,
    created_at DATETIME2(3) NOT NULL,
    updated_at DATETIME2(3) NOT NULL
);
GO

IF OBJECT_ID(N'finandata.dq_rejections', N'U') IS NULL
CREATE TABLE finandata.dq_rejections (
    rejection_id VARCHAR(140) NOT NULL PRIMARY KEY,
    batch_id VARCHAR(80) NOT NULL,
    transaction_id VARCHAR(40) NULL,
    source_file VARCHAR(260) NOT NULL,
    source_row INT NOT NULL,
    rejection_reasons VARCHAR(500) NOT NULL,
    raw_record_json NVARCHAR(MAX) NOT NULL,
    rejected_at DATETIME2(3) NOT NULL
);
GO

IF OBJECT_ID(N'finandata.regulatory_reports', N'U') IS NULL
CREATE TABLE finandata.regulatory_reports (
    batch_id VARCHAR(80) NOT NULL PRIMARY KEY,
    report_status VARCHAR(20) NOT NULL,
    transaction_count INT NOT NULL,
    total_debit DECIMAL(18,2) NOT NULL,
    total_credit DECIMAL(18,2) NOT NULL,
    generated_at DATETIME2(3) NOT NULL
);
GO

-- Historial append-only: el pipeline solo inserta eventos, nunca los actualiza.
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

CREATE INDEX IX_transaction_audit_history_transaction
    ON finandata.transaction_audit_history (transaction_id, changed_at DESC);
CREATE INDEX IX_transaction_audit_history_batch
    ON finandata.transaction_audit_history (batch_id, event_type);
GO
