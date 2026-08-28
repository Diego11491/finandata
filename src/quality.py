from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd


REQUIRED_COLUMNS = [
    "transaction_id",
    "account_from",
    "account_to",
    "iban_from",
    "iban_to",
    "amount",
    "currency",
    "transaction_ts",
    "branch_id",
    "channel",
    "customer_document",
    "card_number",
    "updated_at",
    "cdc_operation",
]


@dataclass
class QualityResult:
    accepted: pd.DataFrame
    rejected: pd.DataFrame
    summary: dict[str, Any]


def _blank(series: pd.Series) -> pd.Series:
    return series.isna() | series.astype("string").str.strip().eq("")


def validate_transactions(data: pd.DataFrame) -> QualityResult:
    """Valida cada fila y conserva todas sus causas de rechazo."""
    missing_columns = sorted(set(REQUIRED_COLUMNS) - set(data.columns))
    if missing_columns:
        raise ValueError(f"Esquema incompleto. Faltan columnas: {missing_columns}")

    checked = data.copy().reset_index(drop=True)
    reasons: list[list[str]] = [[] for _ in range(len(checked))]
    rule_failures: dict[str, int] = {}

    def flag(mask: pd.Series, rule_code: str) -> None:
        normalized = mask.fillna(True).astype(bool)
        affected = int(normalized.sum())
        rule_failures[rule_code] = affected
        if affected:
            for index in checked.index[normalized]:
                reasons[index].append(rule_code)

    for column in REQUIRED_COLUMNS:
        flag(_blank(checked[column]), f"REQUIRED_{column.upper()}")

    transaction_id = checked["transaction_id"].fillna("").astype(str)
    flag(
        ~transaction_id.str.match(r"^TXN-\d{8}-\d{6}$"),
        "INVALID_TRANSACTION_ID",
    )
    flag(transaction_id.duplicated(keep="first"), "DUPLICATE_TRANSACTION_ID")

    amount = pd.to_numeric(checked["amount"], errors="coerce")
    flag(amount.isna() | amount.lt(0), "NEGATIVE_OR_INVALID_AMOUNT")

    iban_pattern = r"^[A-Z]{2}\d{2}[A-Z0-9]{11,30}$"
    flag(
        ~checked["iban_from"].fillna("").astype(str).str.match(iban_pattern),
        "INVALID_IBAN_FROM",
    )
    flag(
        ~checked["iban_to"].fillna("").astype(str).str.match(iban_pattern),
        "INVALID_IBAN_TO",
    )

    flag(
        checked["account_from"].fillna("").astype(str)
        == checked["account_to"].fillna("").astype(str),
        "SAME_DEBIT_CREDIT_ACCOUNT",
    )
    flag(~checked["currency"].isin(["PEN", "USD"]), "UNSUPPORTED_CURRENCY")
    flag(~checked["channel"].isin(["ATM", "MOBILE", "ACH"]), "INVALID_CHANNEL")
    flag(~checked["cdc_operation"].isin(["I", "U"]), "INVALID_CDC_OPERATION")
    flag(
        pd.to_datetime(checked["transaction_ts"], errors="coerce", utc=True).isna(),
        "INVALID_TRANSACTION_TIMESTAMP",
    )
    flag(
        pd.to_datetime(checked["updated_at"], errors="coerce", utc=True).isna(),
        "INVALID_UPDATED_AT",
    )

    checked["_rejection_reasons"] = ["|".join(items) for items in reasons]
    rejected_mask = checked["_rejection_reasons"].ne("")
    rejected = checked.loc[rejected_mask].copy()
    accepted = checked.loc[~rejected_mask].drop(columns=["_rejection_reasons"]).copy()

    total = len(checked)
    rejected_count = len(rejected)
    summary = {
        "total_records": total,
        "accepted_records": len(accepted),
        "rejected_records": rejected_count,
        "rejection_rate": rejected_count / total if total else 0.0,
        "critical_rejected_records": rejected_count,
        "rule_failures": {key: value for key, value in rule_failures.items() if value},
    }
    return QualityResult(accepted=accepted, rejected=rejected, summary=summary)


def evaluate_quality_gate(summary: dict[str, Any], rejection_threshold: float) -> dict[str, Any]:
    critical_ok = summary["critical_rejected_records"] == 0
    rate_ok = summary["rejection_rate"] <= rejection_threshold
    return {
        "passed": bool(critical_ok and rate_ok),
        "critical_rules_passed": critical_ok,
        "rejection_threshold_passed": rate_ok,
        "configured_threshold": rejection_threshold,
        **summary,
    }

