"""Ejecuta las tres pruebas que se mostrarán durante la exposición."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from flows.pipeline import financial_dataops_pipeline
from scripts.generate_data import main as generate_data
from scripts.reset_demo import reset_demo
from src.exceptions import DataQualityGateError
from src.warehouse import (
    count_audit_events,
    count_regulatory_reports,
    count_rejections,
    get_batch_status,
)


def main() -> None:
    reset_demo()
    generate_data()

    first = financial_dataops_pipeline(
        scenario="healthy", force_reprocess=True, batch_id="DEMO-HEALTHY-001"
    )
    assert first["status"] == "PUBLISHED"
    assert first["merge"]["inserted_records"] == 300
    assert first["merge"]["updated_records"] == 0
    assert first["merge"]["audit_events"] == 300

    second = financial_dataops_pipeline(
        scenario="healthy", force_reprocess=True, batch_id="DEMO-HEALTHY-002"
    )
    assert second["status"] == "PUBLISHED"
    assert second["merge"]["inserted_records"] == 0
    assert second["merge"]["updated_records"] == 300
    assert second["merge"]["business_changed_records"] == 0
    assert second["merge"]["reprocessed_records"] == 300
    assert second["merge"]["audit_events"] == 300

    try:
        financial_dataops_pipeline(
            scenario="incident", force_reprocess=True, batch_id="DEMO-INCIDENT-15PCT"
        )
    except DataQualityGateError:
        pass
    else:
        raise AssertionError("El incidente debió ser bloqueado por el Quality Gate")

    incident_status = get_batch_status("DEMO-INCIDENT-15PCT")
    incident_report_count = count_regulatory_reports("DEMO-INCIDENT-15PCT")
    incident_rejections = count_rejections("DEMO-INCIDENT-15PCT")

    assert incident_status == "BLOCKED_QUALITY_GATE"
    assert incident_report_count == 0
    assert incident_rejections == 45
    assert count_audit_events("DEMO-HEALTHY-001", "INSERT") == 300
    assert count_audit_events("DEMO-HEALTHY-002", "REPROCESSED") == 300
    assert count_audit_events("DEMO-INCIDENT-15PCT") == 0
    print("\nDEMO VALIDADA")
    print("1) Azure SQL: 300 INSERT y 300 eventos de auditoría")
    print("2) Azure SQL: 300 reprocesadas sin cambio de negocio ni duplicados")
    print("3) Incidente: 45/300 rechazadas (15%); reporte SBS bloqueado")
    print("4) Auditoría: el incidente bloqueado generó 0 cambios en Gold")


if __name__ == "__main__":
    main()
