from scripts.generate_data import build_base_dataset
from src.transformations import transform_transactions
from src.warehouse import (
    FACT_COLUMNS,
    MERGE_TRANSACTIONS_SQL,
    _normalize_fact_rows,
    _schema_statements,
    classify_audit_event,
)


def test_azure_merge_is_idempotent_by_transaction_id():
    source = build_base_dataset().head(10).copy()
    source["_source_file"] = "test.csv"
    source["_source_row"] = range(2, 12)

    transformed = transform_transactions(source, "BATCH-001")
    rows = _normalize_fact_rows(transformed)

    assert len(rows) == 10
    assert all(len(row) == len(FACT_COLUMNS) for row in rows)
    assert "ON target.transaction_id = source.transaction_id" in MERGE_TRANSACTIONS_SQL
    assert MERGE_TRANSACTIONS_SQL.count("WHEN MATCHED") == 1
    assert "WHEN target.record_hash <> source.record_hash" in MERGE_TRANSACTIONS_SQL
    assert "ELSE target.last_modified_at" in MERGE_TRANSACTIONS_SQL
    assert "ELSE target.last_modified_batch_id" in MERGE_TRANSACTIONS_SQL
    assert "WHEN NOT MATCHED THEN INSERT" in MERGE_TRANSACTIONS_SQL
    assert "created_batch_id" in MERGE_TRANSACTIONS_SQL
    assert "last_modified_batch_id" in MERGE_TRANSACTIONS_SQL


def test_audit_classifies_insert_update_and_reprocessing():
    source = build_base_dataset().head(1).copy()
    source["_source_file"] = "test.csv"
    source["_source_row"] = 2

    first_row = _normalize_fact_rows(
        transform_transactions(source, "BATCH-001")
    )[0]
    reprocessed_row = _normalize_fact_rows(
        transform_transactions(source, "BATCH-002")
    )[0]
    changed_source = source.copy()
    changed_source.loc[changed_source.index[0], "amount"] += 100
    changed_row = _normalize_fact_rows(
        transform_transactions(changed_source, "BATCH-003")
    )[0]

    first = dict(zip(FACT_COLUMNS, first_row, strict=True))
    reprocessed = dict(zip(FACT_COLUMNS, reprocessed_row, strict=True))
    changed = dict(zip(FACT_COLUMNS, changed_row, strict=True))

    assert classify_audit_event(None, first)[0] == "INSERT"
    assert classify_audit_event(first, reprocessed) == ("REPROCESSED", [])
    event_type, changed_columns = classify_audit_event(first, changed)
    assert event_type == "UPDATE"
    assert "amount" in changed_columns


def test_v1_migration_adds_columns_before_using_them():
    statements = _schema_statements("finandata")
    add_fact = next(
        index
        for index, statement in enumerate(statements)
        if "ADD record_hash" in statement
    )
    backfill_fact = next(
        index
        for index, statement in enumerate(statements)
        if "SET created_at = COALESCE(created_at, processed_at)" in statement
    )
    add_batch = next(
        index
        for index, statement in enumerate(statements)
        if "ADD created_at DATETIME2(3) NULL" in statement
        and "etl_batches" in statement
    )
    backfill_batch = next(
        index
        for index, statement in enumerate(statements)
        if "SET created_at = COALESCE(created_at, updated_at)" in statement
    )

    assert add_fact < backfill_fact
    assert add_batch < backfill_batch
