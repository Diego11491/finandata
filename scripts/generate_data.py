"""Genera dos lotes reproducibles: sano y crítico con 15% de rechazo."""

from __future__ import annotations

import json
from datetime import timedelta
from pathlib import Path

import numpy as np
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
INPUT_ROOT = PROJECT_ROOT / "data" / "input"
TOTAL_RECORDS = 300
INCIDENT_REJECTS = 45


def _iban(sequence: int) -> str:
    return f"PE{10 + sequence % 89:02d}FINA{sequence:016d}"


def build_base_dataset() -> pd.DataFrame:
    """Crea 300 transferencias financieras sintéticas y válidas."""
    rng = np.random.default_rng(20260825)
    start = pd.Timestamp("2026-08-25 06:00:00", tz="America/Lima")
    branches = ["LIM001"] * 80 + ["AQP001"] * 80 + ["TRU001"] * 80

    rows: list[dict[str, object]] = []
    for index in range(TOTAL_RECORDS):
        source_sequence = 100_000 + index
        target_sequence = 700_000 + index
        is_atm = index < 240
        channel = "ATM" if is_atm else ("MOBILE" if index % 2 == 0 else "ACH")
        branch_id = branches[index] if is_atm else "DIGITAL"
        transaction_ts = start + timedelta(minutes=index * 2)
        amount = round(float(rng.uniform(20, 25_000)), 2)

        rows.append(
            {
                "transaction_id": f"TXN-20260825-{index + 1:06d}",
                "account_from": f"ACC-{source_sequence}",
                "account_to": f"ACC-{target_sequence}",
                "iban_from": _iban(source_sequence),
                "iban_to": _iban(target_sequence),
                "amount": amount,
                "currency": "PEN" if index % 10 else "USD",
                "transaction_ts": transaction_ts.isoformat(),
                "branch_id": branch_id,
                "channel": channel,
                "customer_document": f"{70_000_000 + index:08d}",
                "card_number": f"45560000{index:08d}",
                "updated_at": (transaction_ts + timedelta(minutes=1)).isoformat(),
                "cdc_operation": "I",
            }
        )

    return pd.DataFrame(rows)


def inject_incident(valid_data: pd.DataFrame) -> pd.DataFrame:
    """Introduce 45 filas inválidas distintas, equivalentes al 15% del lote."""
    incident = valid_data.copy(deep=True)
    bad_indices = np.linspace(0, TOTAL_RECORDS - 1, INCIDENT_REJECTS, dtype=int)

    negative_amount = bad_indices[:15]
    missing_iban = bad_indices[15:25]
    duplicate_id = bad_indices[25:35]
    same_account = bad_indices[35:45]

    incident.loc[negative_amount, "amount"] *= -1
    incident.loc[missing_iban, "iban_from"] = ""

    # Los objetivos están antes en el lote: duplicated(keep="first") rechazará
    # exclusivamente las 10 filas seleccionadas y conservará un total exacto de 45.
    duplicate_targets = list(range(10))
    for row_index, target_index in zip(duplicate_id, duplicate_targets, strict=True):
        incident.loc[row_index, "transaction_id"] = incident.loc[target_index, "transaction_id"]

    incident.loc[same_account, "account_to"] = incident.loc[same_account, "account_from"]
    return incident


def write_scenario(name: str, data: pd.DataFrame) -> None:
    scenario_root = INPUT_ROOT / name
    atm_root = scenario_root / "atm"
    atm_root.mkdir(parents=True, exist_ok=True)

    atm_rows = data[data["channel"] == "ATM"]
    for branch_id, frame in atm_rows.groupby("branch_id"):
        frame.to_csv(atm_root / f"atm_{branch_id}.csv", index=False)

    api_rows = data[data["channel"] != "ATM"]
    records = json.loads(api_rows.to_json(orient="records"))
    (scenario_root / "api_transactions.json").write_text(
        json.dumps(records, indent=2, ensure_ascii=False), encoding="utf-8"
    )


def main() -> None:
    healthy = build_base_dataset()
    incident = inject_incident(healthy)
    write_scenario("healthy", healthy)
    write_scenario("incident", incident)

    assert len(healthy) == TOTAL_RECORDS
    assert len(incident) == TOTAL_RECORDS
    print("Datos generados: 300 filas sanas y 300 filas con 45 errores (15%).")


if __name__ == "__main__":
    main()
