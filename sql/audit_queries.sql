-- 1. Inventario seguro: tablas y cantidad aproximada de filas.
SELECT
    s.name AS schema_name,
    t.name AS table_name,
    SUM(p.rows) AS rows_count
FROM sys.tables AS t
INNER JOIN sys.schemas AS s ON s.schema_id = t.schema_id
INNER JOIN sys.partitions AS p
    ON p.object_id = t.object_id AND p.index_id IN (0, 1)
WHERE s.name = N'finandata'
GROUP BY s.name, t.name
ORDER BY t.name;

-- 2. Últimos lotes y su decisión de control.
SELECT TOP (10)
    batch_id, scenario, status, total_records, accepted_records,
    rejected_records,
    CAST(rejection_rate * 100 AS DECIMAL(5,2)) AS rejection_pct,
    inserted_records, updated_records, flow_run_id, git_commit,
    created_at, updated_at
FROM finandata.etl_batches
ORDER BY updated_at DESC;

-- 3. Resumen del historial: 300 INSERT + 300 REPROCESSED en la demo.
SELECT event_type, COUNT(*) AS events
FROM finandata.transaction_audit_history
GROUP BY event_type
ORDER BY event_type;

-- 4. Trazabilidad completa de una transacción sin mostrar PII en claro.
DECLARE @transaction_id VARCHAR(40) = (
    SELECT TOP (1) transaction_id
    FROM finandata.fact_transactions
    ORDER BY transaction_id
);

SELECT TOP (20)
    audit_id, transaction_id, event_type, batch_id, flow_run_id,
    pipeline_version, previous_record_hash, new_record_hash,
    changed_columns_json, source_file, source_row, changed_at
FROM finandata.transaction_audit_history
WHERE transaction_id = @transaction_id
ORDER BY changed_at DESC, audit_id DESC;

-- 5. Evidencia de que el lote bloqueado no modificó Gold.
SELECT COUNT(*) AS audit_events_from_blocked_batch
FROM finandata.transaction_audit_history
WHERE batch_id = 'DEMO-INCIDENT-15PCT';

-- 6. Rechazos del incidente, agrupados para no usar SELECT *.
SELECT rejection_reasons, COUNT(*) AS rejected_records
FROM finandata.dq_rejections
WHERE batch_id = 'DEMO-INCIDENT-15PCT'
GROUP BY rejection_reasons
ORDER BY rejected_records DESC;
