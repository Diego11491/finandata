from __future__ import annotations

import json
import os
import re
from contextlib import contextmanager
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Iterator
from uuid import uuid4

import pandas as pd

from src.exceptions import ReconciliationError
from src.settings import load_settings


FACT_COLUMNS = [
    "transaction_id",
    "account_from",
    "account_to",
    "amount",
    "currency",
    "transaction_ts",
    "branch_id",
    "channel",
    "commission",
    "risk_score",
    "risk_level",
    "customer_document_hash",
    "card_masked",
    "iban_from_masked",
    "iban_to_masked",
    "debit_amount",
    "credit_amount",
    "batch_id",
    "processed_at",
    "source_file",
    "source_row",
    "record_hash",
]

AUDIT_COMPARE_COLUMNS = [
    column
    for column in FACT_COLUMNS
    if column not in {"batch_id", "processed_at", "record_hash"}
]


def _load_pyodbc():
    try:
        import pyodbc
    except ImportError as exc:
        raise RuntimeError(
            "No se pudo cargar pyodbc o la biblioteca ODBC del sistema. "
            "Ejecuta pip install -r requirements.txt e instala Microsoft "
            "ODBC Driver 18 for SQL Server."
        ) from exc
    return pyodbc


def _required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(
            f"Falta {name} en .env. Copia .env.example como .env y complétalo."
        )
    return value


def _odbc_value(value: str) -> str:
    """Protege valores de conexión que contienen ; o llaves."""
    return "{" + value.replace("}", "}}") + "}"


def warehouse_schema() -> str:
    schema = os.getenv(
        "AZURE_SQL_SCHEMA", load_settings()["warehouse"]["schema"]
    ).strip()
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", schema):
        raise RuntimeError("AZURE_SQL_SCHEMA contiene caracteres no permitidos.")
    return schema


def build_connection_string() -> str:
    # load_settings también carga el archivo .env del proyecto.
    load_settings()
    server = _required_env("AZURE_SQL_SERVER")
    database = _required_env("AZURE_SQL_DATABASE")
    username = _required_env("AZURE_SQL_USERNAME")
    password = _required_env("AZURE_SQL_PASSWORD")
    driver = os.getenv(
        "AZURE_SQL_DRIVER", "ODBC Driver 18 for SQL Server"
    ).strip()

    if server.lower().startswith("tcp:"):
        endpoint = server
    elif "," in server:
        endpoint = f"tcp:{server}"
    else:
        endpoint = f"tcp:{server},1433"

    return ";".join(
        [
            f"DRIVER={_odbc_value(driver)}",
            f"SERVER={endpoint}",
            f"DATABASE={_odbc_value(database)}",
            f"UID={_odbc_value(username)}",
            f"PWD={_odbc_value(password)}",
            "Encrypt=yes",
            "TrustServerCertificate=no",
            "Connection Timeout=30",
        ]
    ) + ";"


def connect():
    connection_string = build_connection_string()
    pyodbc = _load_pyodbc()
    driver = os.getenv(
        "AZURE_SQL_DRIVER", "ODBC Driver 18 for SQL Server"
    ).strip()
    available_drivers = pyodbc.drivers()
    if driver not in available_drivers:
        available = ", ".join(available_drivers) or "ninguno"
        raise RuntimeError(
            f"No está instalado '{driver}'. Drivers detectados: {available}. "
            "Instala Microsoft ODBC Driver 18 for SQL Server."
        )
    try:
        return pyodbc.connect(connection_string, autocommit=False)
    except pyodbc.Error as exc:
        raise RuntimeError(
            "No se pudo conectar con Azure SQL. Revisa servidor, base de datos, "
            "usuario, contraseña y la regla de firewall para tu IP. "
            f"Detalle técnico: {exc}"
        ) from exc


def _qualified(schema: str, table: str) -> str:
    return f"[{schema}].[{table}]"


def _schema_statements(schema: str) -> list[str]:
    fact = _qualified(schema, "fact_transactions")
    batches = _qualified(schema, "etl_batches")
    rejections = _qualified(schema, "dq_rejections")
    reports = _qualified(schema, "regulatory_reports")
    audit = _qualified(schema, "transaction_audit_history")
    return [
        f"IF SCHEMA_ID(N'{schema}') IS NULL EXEC(N'CREATE SCHEMA [{schema}]')",
        f"""
        IF OBJECT_ID(N'{fact}', N'U') IS NULL
        CREATE TABLE {fact} (
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
        )
        """,
        # SQL Server compila cada llamada a execute como un lote completo. Las
        # columnas deben agregarse en un lote anterior al UPDATE que las usa;
        # de lo contrario, una base v1.0 falla con "Invalid column name".
        f"""
        IF COL_LENGTH(N'{schema}.fact_transactions', N'record_hash') IS NULL
            ALTER TABLE {fact} ADD record_hash CHAR(64) NULL;
        IF COL_LENGTH(N'{schema}.fact_transactions', N'created_at') IS NULL
            ALTER TABLE {fact} ADD created_at DATETIME2(3) NULL;
        IF COL_LENGTH(N'{schema}.fact_transactions', N'last_modified_at') IS NULL
            ALTER TABLE {fact} ADD last_modified_at DATETIME2(3) NULL;
        IF COL_LENGTH(N'{schema}.fact_transactions', N'created_batch_id') IS NULL
            ALTER TABLE {fact} ADD created_batch_id VARCHAR(80) NULL;
        IF COL_LENGTH(N'{schema}.fact_transactions', N'last_modified_batch_id') IS NULL
            ALTER TABLE {fact} ADD last_modified_batch_id VARCHAR(80) NULL;
        """,
        f"""
        UPDATE {fact}
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
        """,
        f"""
        IF EXISTS (
            SELECT 1 FROM sys.columns
            WHERE object_id = OBJECT_ID(N'{fact}')
              AND name = N'record_hash' AND is_nullable = 1
        ) ALTER TABLE {fact} ALTER COLUMN record_hash CHAR(64) NOT NULL;
        IF EXISTS (
            SELECT 1 FROM sys.columns
            WHERE object_id = OBJECT_ID(N'{fact}')
              AND name = N'created_at' AND is_nullable = 1
        ) ALTER TABLE {fact} ALTER COLUMN created_at DATETIME2(3) NOT NULL;
        IF EXISTS (
            SELECT 1 FROM sys.columns
            WHERE object_id = OBJECT_ID(N'{fact}')
              AND name = N'last_modified_at' AND is_nullable = 1
        ) ALTER TABLE {fact} ALTER COLUMN last_modified_at DATETIME2(3) NOT NULL;
        IF EXISTS (
            SELECT 1 FROM sys.columns
            WHERE object_id = OBJECT_ID(N'{fact}')
              AND name = N'created_batch_id' AND is_nullable = 1
        ) ALTER TABLE {fact} ALTER COLUMN created_batch_id VARCHAR(80) NOT NULL;
        IF EXISTS (
            SELECT 1 FROM sys.columns
            WHERE object_id = OBJECT_ID(N'{fact}')
              AND name = N'last_modified_batch_id' AND is_nullable = 1
        ) ALTER TABLE {fact} ALTER COLUMN last_modified_batch_id VARCHAR(80) NOT NULL;
        """,
        f"""
        IF OBJECT_ID(N'{batches}', N'U') IS NULL
        CREATE TABLE {batches} (
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
        )
        """,
        f"""
        IF COL_LENGTH(N'{schema}.etl_batches', N'created_at') IS NULL
            ALTER TABLE {batches} ADD created_at DATETIME2(3) NULL;
        """,
        f"""
        UPDATE {batches}
        SET created_at = COALESCE(created_at, updated_at)
        WHERE created_at IS NULL;
        """,
        f"""
        IF EXISTS (
            SELECT 1 FROM sys.columns
            WHERE object_id = OBJECT_ID(N'{batches}')
              AND name = N'created_at' AND is_nullable = 1
        ) ALTER TABLE {batches} ALTER COLUMN created_at DATETIME2(3) NOT NULL;
        """,
        f"""
        IF OBJECT_ID(N'{rejections}', N'U') IS NULL
        CREATE TABLE {rejections} (
            rejection_id VARCHAR(140) NOT NULL PRIMARY KEY,
            batch_id VARCHAR(80) NOT NULL,
            transaction_id VARCHAR(40) NULL,
            source_file VARCHAR(260) NOT NULL,
            source_row INT NOT NULL,
            rejection_reasons VARCHAR(500) NOT NULL,
            raw_record_json NVARCHAR(MAX) NOT NULL,
            rejected_at DATETIME2(3) NOT NULL
        )
        """,
        f"""
        IF OBJECT_ID(N'{reports}', N'U') IS NULL
        CREATE TABLE {reports} (
            batch_id VARCHAR(80) NOT NULL PRIMARY KEY,
            report_status VARCHAR(20) NOT NULL,
            transaction_count INT NOT NULL,
            total_debit DECIMAL(18,2) NOT NULL,
            total_credit DECIMAL(18,2) NOT NULL,
            generated_at DATETIME2(3) NOT NULL
        )
        """,
        f"""
        IF OBJECT_ID(N'{audit}', N'U') IS NULL
        CREATE TABLE {audit} (
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
        )
        """,
        f"""
        IF NOT EXISTS (
            SELECT 1 FROM sys.indexes
            WHERE object_id = OBJECT_ID(N'{audit}')
              AND name = N'IX_transaction_audit_history_transaction'
        ) CREATE INDEX IX_transaction_audit_history_transaction
            ON {audit} (transaction_id, changed_at DESC);
        IF NOT EXISTS (
            SELECT 1 FROM sys.indexes
            WHERE object_id = OBJECT_ID(N'{audit}')
              AND name = N'IX_transaction_audit_history_batch'
        ) CREATE INDEX IX_transaction_audit_history_batch
            ON {audit} (batch_id, event_type);
        """,
    ]


def initialize_schema(connection) -> None:
    cursor = connection.cursor()
    try:
        for statement in _schema_statements(warehouse_schema()):
            cursor.execute(statement)
        connection.commit()
    finally:
        cursor.close()


@contextmanager
def warehouse_connection() -> Iterator[Any]:
    connection = connect()
    try:
        initialize_schema(connection)
        yield connection
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def test_connection() -> dict[str, str]:
    with warehouse_connection() as connection:
        cursor = connection.cursor()
        try:
            row = cursor.execute(
                "SELECT CAST(SERVERPROPERTY('ServerName') AS NVARCHAR(128)), "
                "DB_NAME(), SUSER_SNAME()"
            ).fetchone()
            # Smoke test sin filas: valida en el motor real la sintaxis del
            # MERGE sin insertar, actualizar ni eliminar datos.
            _create_staging_table(cursor)
            cursor.execute(merge_transactions_sql(warehouse_schema()))
        finally:
            cursor.close()
    return {"server": str(row[0]), "database": str(row[1]), "user": str(row[2])}


def _utc_now_naive() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _utc_naive(value: object) -> datetime:
    timestamp = pd.to_datetime(value, utc=True)
    return timestamp.to_pydatetime().replace(tzinfo=None)


def _decimal(value: object) -> Decimal:
    return Decimal(str(value)).quantize(Decimal("0.01"))


def record_batch(
    batch_id: str,
    scenario: str,
    status: str,
    summary: dict[str, Any],
    git_commit: str,
    flow_run_id: str = "local",
    merge_stats: dict[str, int] | None = None,
) -> None:
    merge_stats = merge_stats or {"inserted_records": 0, "updated_records": 0}
    table = _qualified(warehouse_schema(), "etl_batches")
    sql = f"""
        MERGE {table} AS target
        USING (VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)) AS source (
            batch_id, scenario, flow_run_id, status, total_records,
            accepted_records, rejected_records, rejection_rate,
            inserted_records, updated_records, git_commit, detail_json,
            created_at, updated_at
        )
        ON target.batch_id = source.batch_id
        WHEN MATCHED THEN UPDATE SET
            scenario = source.scenario,
            flow_run_id = source.flow_run_id,
            status = source.status,
            total_records = source.total_records,
            accepted_records = source.accepted_records,
            rejected_records = source.rejected_records,
            rejection_rate = source.rejection_rate,
            inserted_records = source.inserted_records,
            updated_records = source.updated_records,
            git_commit = source.git_commit,
            detail_json = source.detail_json,
            updated_at = source.updated_at
        WHEN NOT MATCHED THEN INSERT (
            batch_id, scenario, flow_run_id, status, total_records,
            accepted_records, rejected_records, rejection_rate,
            inserted_records, updated_records, git_commit, detail_json,
            created_at, updated_at
        ) VALUES (
            source.batch_id, source.scenario, source.flow_run_id, source.status,
            source.total_records, source.accepted_records, source.rejected_records,
            source.rejection_rate, source.inserted_records, source.updated_records,
            source.git_commit, source.detail_json, source.created_at,
            source.updated_at
        );
    """
    now = _utc_now_naive()
    detail = {"quality": summary, "merge": merge_stats}
    values = (
        batch_id,
        scenario,
        flow_run_id,
        status,
        int(summary.get("total_records", 0)),
        int(summary.get("accepted_records", 0)),
        int(summary.get("rejected_records", 0)),
        float(summary.get("rejection_rate", 0.0)),
        int(merge_stats.get("inserted_records", 0)),
        int(merge_stats.get("updated_records", 0)),
        git_commit,
        json.dumps(detail, default=str, ensure_ascii=False),
        now,
        now,
    )
    with warehouse_connection() as connection:
        cursor = connection.cursor()
        try:
            cursor.execute(sql, values)
        finally:
            cursor.close()


def record_rejections(batch_id: str, rejected: pd.DataFrame) -> int:
    if rejected.empty:
        return 0
    table = _qualified(warehouse_schema(), "dq_rejections")
    now = _utc_now_naive()
    rows: list[tuple[Any, ...]] = []
    for _, record in rejected.iterrows():
        rows.append(
            (
                f"REJ-{batch_id}-{uuid4().hex[:12]}",
                batch_id,
                None
                if pd.isna(record.get("transaction_id"))
                else str(record.get("transaction_id")),
                str(record.get("_source_file", "unknown")),
                int(record.get("_source_row", -1)),
                str(record["_rejection_reasons"]),
                json.dumps(record.to_dict(), default=str, ensure_ascii=False),
                now,
            )
        )
    with warehouse_connection() as connection:
        cursor = connection.cursor()
        try:
            cursor.execute(f"DELETE FROM {table} WHERE batch_id = ?", batch_id)
            cursor.executemany(
                f"""
                INSERT INTO {table} (
                    rejection_id, batch_id, transaction_id, source_file, source_row,
                    rejection_reasons, raw_record_json, rejected_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
        finally:
            cursor.close()
    return len(rows)


def _normalize_fact_rows(data: pd.DataFrame) -> list[tuple[Any, ...]]:
    staging = data.rename(
        columns={"_source_file": "source_file", "_source_row": "source_row"}
    )[FACT_COLUMNS].copy()
    rows: list[tuple[Any, ...]] = []
    for record in staging.to_dict(orient="records"):
        rows.append(
            (
                str(record["transaction_id"]),
                str(record["account_from"]),
                str(record["account_to"]),
                _decimal(record["amount"]),
                str(record["currency"]),
                _utc_naive(record["transaction_ts"]),
                str(record["branch_id"]),
                str(record["channel"]),
                _decimal(record["commission"]),
                int(record["risk_score"]),
                str(record["risk_level"]),
                str(record["customer_document_hash"]),
                str(record["card_masked"]),
                str(record["iban_from_masked"]),
                str(record["iban_to_masked"]),
                _decimal(record["debit_amount"]),
                _decimal(record["credit_amount"]),
                str(record["batch_id"]),
                _utc_naive(record["processed_at"]),
                str(record["source_file"]),
                int(record["source_row"]),
                str(record["record_hash"]),
            )
        )
    return rows


STAGING_TABLE_SQL = """
    CREATE TABLE #staging_transactions (
        transaction_id VARCHAR(40) NOT NULL,
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
        record_hash CHAR(64) NOT NULL
    )
"""


def _create_staging_table(cursor) -> None:
    cursor.execute(STAGING_TABLE_SQL)


def merge_transactions_sql(schema: str = "finandata") -> str:
    target = _qualified(schema, "fact_transactions")
    return f"""
        MERGE {target} AS target
        USING #staging_transactions AS source
        ON target.transaction_id = source.transaction_id
        WHEN MATCHED THEN UPDATE SET
            account_from = source.account_from,
            account_to = source.account_to,
            amount = source.amount,
            currency = source.currency,
            transaction_ts = source.transaction_ts,
            branch_id = source.branch_id,
            channel = source.channel,
            commission = source.commission,
            risk_score = source.risk_score,
            risk_level = source.risk_level,
            customer_document_hash = source.customer_document_hash,
            card_masked = source.card_masked,
            iban_from_masked = source.iban_from_masked,
            iban_to_masked = source.iban_to_masked,
            debit_amount = source.debit_amount,
            credit_amount = source.credit_amount,
            batch_id = source.batch_id,
            processed_at = source.processed_at,
            source_file = source.source_file,
            source_row = source.source_row,
            record_hash = source.record_hash,
            last_modified_at = CASE
                WHEN target.record_hash <> source.record_hash
                    THEN source.processed_at
                ELSE target.last_modified_at
            END,
            last_modified_batch_id = CASE
                WHEN target.record_hash <> source.record_hash
                    THEN source.batch_id
                ELSE target.last_modified_batch_id
            END
        WHEN NOT MATCHED THEN INSERT (
            transaction_id, account_from, account_to, amount, currency,
            transaction_ts, branch_id, channel, commission, risk_score,
            risk_level, customer_document_hash, card_masked, iban_from_masked,
            iban_to_masked, debit_amount, credit_amount, batch_id, processed_at,
            source_file, source_row, record_hash, created_at, last_modified_at,
            created_batch_id, last_modified_batch_id
        ) VALUES (
            source.transaction_id, source.account_from, source.account_to,
            source.amount, source.currency, source.transaction_ts, source.branch_id,
            source.channel, source.commission, source.risk_score, source.risk_level,
            source.customer_document_hash, source.card_masked,
            source.iban_from_masked, source.iban_to_masked, source.debit_amount,
            source.credit_amount, source.batch_id, source.processed_at,
            source.source_file, source.source_row, source.record_hash,
            source.processed_at, source.processed_at, source.batch_id,
            source.batch_id
        );
    """


MERGE_TRANSACTIONS_SQL = merge_transactions_sql()


def _comparison_value(value: Any) -> str | None:
    if value is None or pd.isna(value):
        return None
    if isinstance(value, datetime):
        return value.isoformat(timespec="milliseconds")
    if isinstance(value, Decimal):
        return format(value, "f")
    return str(value)


def classify_audit_event(
    before: dict[str, Any] | None, after: dict[str, Any]
) -> tuple[str, list[str]]:
    """Clasifica inserción, cambio real o reprocesamiento sin cambio de negocio."""
    if before is None:
        return "INSERT", list(AUDIT_COMPARE_COLUMNS)
    if before.get("record_hash") == after.get("record_hash"):
        return "REPROCESSED", []
    changed = [
        column
        for column in AUDIT_COMPARE_COLUMNS
        if _comparison_value(before.get(column))
        != _comparison_value(after.get(column))
    ]
    return "UPDATE", changed


def _audit_json(record: dict[str, Any] | None) -> str | None:
    if record is None:
        return None
    return json.dumps(record, default=str, ensure_ascii=False, sort_keys=True)


def merge_transactions(
    data: pd.DataFrame,
    flow_run_id: str = "local",
    pipeline_version: str = "local-dev",
) -> dict[str, int]:
    rows = _normalize_fact_rows(data)
    if not rows:
        return {
            "inserted_records": 0,
            "updated_records": 0,
            "business_changed_records": 0,
            "reprocessed_records": 0,
            "audit_events": 0,
        }
    target = _qualified(warehouse_schema(), "fact_transactions")
    audit_table = _qualified(warehouse_schema(), "transaction_audit_history")
    with warehouse_connection() as connection:
        cursor = connection.cursor()
        try:
            _create_staging_table(cursor)
            placeholders = ", ".join(["?"] * len(FACT_COLUMNS))
            cursor.executemany(
                f"INSERT INTO #staging_transactions VALUES ({placeholders})", rows
            )
            selected_columns = ", ".join(
                f"target.[{column}]" for column in FACT_COLUMNS
            )
            existing_rows = cursor.execute(
                f"""
                SELECT {selected_columns}
                FROM {target} AS target
                INNER JOIN #staging_transactions AS source
                    ON source.transaction_id = target.transaction_id
                """
            ).fetchall()
            existing_by_id = {
                str(record[0]): dict(zip(FACT_COLUMNS, record, strict=True))
                for record in existing_rows
            }
            after_records = [
                dict(zip(FACT_COLUMNS, record, strict=True)) for record in rows
            ]
            audit_rows: list[tuple[Any, ...]] = []
            event_counts = {"UPDATE": 0, "REPROCESSED": 0}
            for after in after_records:
                transaction_id = str(after["transaction_id"])
                before = existing_by_id.get(transaction_id)
                event_type, changed_columns = classify_audit_event(before, after)
                if event_type in event_counts:
                    event_counts[event_type] += 1
                audit_rows.append(
                    (
                        f"AUD-{uuid4().hex}",
                        transaction_id,
                        event_type,
                        str(after["batch_id"]),
                        flow_run_id,
                        pipeline_version,
                        None if before is None else str(before["record_hash"]),
                        str(after["record_hash"]),
                        json.dumps(changed_columns, ensure_ascii=False),
                        _audit_json(before),
                        _audit_json(after),
                        str(after["source_file"]),
                        int(after["source_row"]),
                        after["processed_at"],
                    )
                )
            cursor.execute(merge_transactions_sql(warehouse_schema()))
            cursor.executemany(
                f"""
                INSERT INTO {audit_table} (
                    audit_id, transaction_id, event_type, batch_id, flow_run_id,
                    pipeline_version, previous_record_hash, new_record_hash,
                    changed_columns_json, before_record_json, after_record_json,
                    source_file, source_row, changed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                audit_rows,
            )
        finally:
            cursor.close()
    matched = len(existing_by_id)
    return {
        "inserted_records": len(rows) - matched,
        "updated_records": matched,
        "business_changed_records": event_counts["UPDATE"],
        "reprocessed_records": event_counts["REPROCESSED"],
        "audit_events": len(audit_rows),
    }


def reconcile_batch(batch_id: str, expected_count: int) -> dict[str, Any]:
    table = _qualified(warehouse_schema(), "fact_transactions")
    with warehouse_connection() as connection:
        cursor = connection.cursor()
        try:
            count, total_debit, total_credit = cursor.execute(
                f"""
                SELECT COUNT(*), COALESCE(SUM(debit_amount), 0),
                       COALESCE(SUM(credit_amount), 0)
                FROM {table}
                WHERE batch_id = ?
                """,
                batch_id,
            ).fetchone()
        finally:
            cursor.close()

    count_ok = int(count) == int(expected_count)
    debit = round(float(total_debit), 2)
    credit = round(float(total_credit), 2)
    balance_ok = debit == credit
    result = {
        "passed": count_ok and balance_ok,
        "expected_count": int(expected_count),
        "warehouse_count": int(count),
        "count_consistent": count_ok,
        "total_debit": debit,
        "total_credit": credit,
        "debit_equals_credit": balance_ok,
    }
    if not result["passed"]:
        raise ReconciliationError(f"Conciliación postcarga fallida: {result}")
    return result


def publish_regulatory_report(
    batch_id: str, reconciliation: dict[str, Any]
) -> dict[str, Any]:
    generated_at = _utc_now_naive()
    report = {
        "batch_id": batch_id,
        "report_status": "AUTHORIZED",
        "transaction_count": int(reconciliation["warehouse_count"]),
        "total_debit": float(reconciliation["total_debit"]),
        "total_credit": float(reconciliation["total_credit"]),
        "generated_at": generated_at.replace(tzinfo=timezone.utc).isoformat(),
    }
    table = _qualified(warehouse_schema(), "regulatory_reports")
    sql = f"""
        MERGE {table} AS target
        USING (VALUES (?, ?, ?, ?, ?, ?)) AS source (
            batch_id, report_status, transaction_count, total_debit,
            total_credit, generated_at
        )
        ON target.batch_id = source.batch_id
        WHEN MATCHED THEN UPDATE SET
            report_status = source.report_status,
            transaction_count = source.transaction_count,
            total_debit = source.total_debit,
            total_credit = source.total_credit,
            generated_at = source.generated_at
        WHEN NOT MATCHED THEN INSERT (
            batch_id, report_status, transaction_count, total_debit,
            total_credit, generated_at
        ) VALUES (
            source.batch_id, source.report_status, source.transaction_count,
            source.total_debit, source.total_credit, source.generated_at
        );
    """
    with warehouse_connection() as connection:
        cursor = connection.cursor()
        try:
            cursor.execute(
                sql,
                batch_id,
                "AUTHORIZED",
                int(reconciliation["warehouse_count"]),
                _decimal(reconciliation["total_debit"]),
                _decimal(reconciliation["total_credit"]),
                generated_at,
            )
        finally:
            cursor.close()
    return report


def reset_demo_data() -> None:
    schema = warehouse_schema()
    with warehouse_connection() as connection:
        cursor = connection.cursor()
        try:
            for table in [
                "transaction_audit_history",
                "regulatory_reports",
                "dq_rejections",
                "etl_batches",
                "fact_transactions",
            ]:
                cursor.execute(f"DELETE FROM {_qualified(schema, table)}")
        finally:
            cursor.close()


def _scalar(sql: str, *params: Any) -> int:
    with warehouse_connection() as connection:
        cursor = connection.cursor()
        try:
            value = cursor.execute(sql, *params).fetchone()[0]
        finally:
            cursor.close()
    return int(value)


def get_batch_status(batch_id: str) -> str | None:
    table = _qualified(warehouse_schema(), "etl_batches")
    with warehouse_connection() as connection:
        cursor = connection.cursor()
        try:
            row = cursor.execute(
                f"SELECT status FROM {table} WHERE batch_id = ?", batch_id
            ).fetchone()
        finally:
            cursor.close()
    return None if row is None else str(row[0])


def count_regulatory_reports(batch_id: str) -> int:
    table = _qualified(warehouse_schema(), "regulatory_reports")
    return _scalar(f"SELECT COUNT(*) FROM {table} WHERE batch_id = ?", batch_id)


def count_rejections(batch_id: str) -> int:
    table = _qualified(warehouse_schema(), "dq_rejections")
    return _scalar(f"SELECT COUNT(*) FROM {table} WHERE batch_id = ?", batch_id)


def count_audit_events(batch_id: str, event_type: str | None = None) -> int:
    table = _qualified(warehouse_schema(), "transaction_audit_history")
    if event_type is None:
        return _scalar(f"SELECT COUNT(*) FROM {table} WHERE batch_id = ?", batch_id)
    return _scalar(
        f"SELECT COUNT(*) FROM {table} WHERE batch_id = ? AND event_type = ?",
        batch_id,
        event_type,
    )


def _to_dataframe(cursor) -> pd.DataFrame:
    columns = [column[0] for column in cursor.description]
    return pd.DataFrame.from_records(cursor.fetchall(), columns=columns)


def fetch_operational_snapshot() -> dict[str, pd.DataFrame]:
    schema = warehouse_schema()
    with warehouse_connection() as connection:
        cursor = connection.cursor()
        try:
            cursor.execute(
                f"""
                SELECT batch_id, scenario, status, total_records, accepted_records,
                       rejected_records,
                       ROUND(CAST(rejection_rate AS FLOAT) * 100, 2) AS rejection_pct,
                       inserted_records, updated_records
                FROM {_qualified(schema, 'etl_batches')}
                ORDER BY updated_at
                """
            )
            batches = _to_dataframe(cursor)
            cursor.execute(
                f"SELECT * FROM {_qualified(schema, 'regulatory_reports')} "
                "ORDER BY generated_at"
            )
            reports = _to_dataframe(cursor)
            cursor.execute(
                f"""
                SELECT rejection_reasons, COUNT(*) AS records
                FROM {_qualified(schema, 'dq_rejections')}
                GROUP BY rejection_reasons
                ORDER BY records DESC
                """
            )
            rejections = _to_dataframe(cursor)
            cursor.execute(
                f"""
                SELECT event_type, COUNT(*) AS events
                FROM {_qualified(schema, 'transaction_audit_history')}
                GROUP BY event_type
                ORDER BY event_type
                """
            )
            audit_summary = _to_dataframe(cursor)
            cursor.execute(
                f"""
                SELECT TOP (20) audit_id, transaction_id, event_type, batch_id,
                       flow_run_id, pipeline_version, changed_columns_json,
                       source_file, source_row, changed_at
                FROM {_qualified(schema, 'transaction_audit_history')}
                ORDER BY changed_at DESC, audit_id DESC
                """
            )
            recent_audit = _to_dataframe(cursor)
        finally:
            cursor.close()
    return {
        "batches": batches,
        "reports": reports,
        "rejections": rejections,
        "audit_summary": audit_summary,
        "recent_audit": recent_audit,
    }
