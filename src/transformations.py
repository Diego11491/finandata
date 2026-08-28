from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

import pandas as pd


COMMISSION_RATE = {"ATM": 0.0080, "MOBILE": 0.0025, "ACH": 0.0045}

# Solo contiene datos ya enmascarados. Se excluyen batch_id y processed_at para
# distinguir un cambio real del negocio de un simple reprocesamiento técnico.
RECORD_HASH_COLUMNS = [
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
]


def _sha256(value: object) -> str:
    return hashlib.sha256(str(value).encode("utf-8")).hexdigest()


def _mask_last_four(value: object) -> str:
    text = str(value)
    return f"{'*' * max(len(text) - 4, 0)}{text[-4:]}"


def _canonical_value(value: Any) -> Any:
    """Normaliza tipos para obtener hashes estables entre ejecuciones."""
    if value is None or (not isinstance(value, (list, dict)) and pd.isna(value)):
        return None
    if isinstance(value, (datetime, pd.Timestamp)):
        return pd.to_datetime(value, utc=True).isoformat()
    if hasattr(value, "item"):
        value = value.item()
    if isinstance(value, float):
        return format(Decimal(str(value)).normalize(), "f")
    return value


def calculate_record_hash(record: pd.Series) -> str:
    payload = {
        column: _canonical_value(record[column]) for column in RECORD_HASH_COLUMNS
    }
    canonical_json = json.dumps(
        payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    )
    return hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()


def transform_transactions(data: pd.DataFrame, batch_id: str) -> pd.DataFrame:
    """Calcula comisión/riesgo y elimina PII en claro antes del Warehouse."""
    transformed = data.copy()
    transformed["amount"] = pd.to_numeric(transformed["amount"])
    transformed["commission"] = (
        transformed["amount"] * transformed["channel"].map(COMMISSION_RATE)
    ).round(2)

    hour = (
        pd.to_datetime(transformed["transaction_ts"], utc=True)
        .dt.tz_convert("America/Lima")
        .dt.hour
    )
    transformed["risk_score"] = (
        transformed["amount"].gt(10_000).astype(int) * 40
        + transformed["amount"].gt(20_000).astype(int) * 20
        + transformed["channel"].eq("ACH").astype(int) * 15
        + transformed["currency"].eq("USD").astype(int) * 15
        + ((hour < 6) | (hour > 22)).astype(int) * 10
    ).clip(upper=100)
    transformed["risk_level"] = pd.cut(
        transformed["risk_score"],
        bins=[-1, 29, 59, 100],
        labels=["LOW", "MEDIUM", "HIGH"],
    ).astype(str)

    transformed["customer_document_hash"] = transformed["customer_document"].map(
        _sha256
    )
    transformed["card_masked"] = transformed["card_number"].map(_mask_last_four)
    transformed["iban_from_masked"] = transformed["iban_from"].map(_mask_last_four)
    transformed["iban_to_masked"] = transformed["iban_to"].map(_mask_last_four)
    transformed["debit_amount"] = transformed["amount"]
    transformed["credit_amount"] = transformed["amount"]
    transformed["batch_id"] = batch_id
    transformed["processed_at"] = datetime.now(timezone.utc).isoformat()

    transformed = transformed.drop(
        columns=["customer_document", "card_number", "iban_from", "iban_to"]
    )
    transformed["record_hash"] = transformed.apply(calculate_record_hash, axis=1)
    return transformed
