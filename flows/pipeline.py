from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from time import perf_counter
from typing import Any
from uuid import uuid4

import pandas as pd
from prefect import flow, get_run_logger, task
from prefect.runtime import flow_run

from src.cdc import apply_cdc, load_watermarks, persist_watermarks
from src.exceptions import DataQualityGateError, ReconciliationError
from src.gx_validation import run_gx_suite
from src.quality import QualityResult, evaluate_quality_gate, validate_transactions
from src.settings import PROJECT_ROOT, load_settings
from src.storage import write_dataframe, write_json_artifact
from src.transformations import transform_transactions
from src.warehouse import (
    merge_transactions,
    publish_regulatory_report,
    reconcile_batch,
    record_batch,
    record_rejections,
)


@task(name="Extract ATM branch", retries=2, retry_delay_seconds=[1, 2], persist_result=False)
def extract_atm_file(
    file_path: str, source_name: str, watermark: str | None, force: bool
) -> pd.DataFrame:
    logger = get_run_logger()
    path = Path(file_path)
    data = pd.read_csv(path)
    data["_source_name"] = source_name
    data["_source_file"] = path.name
    data["_source_row"] = range(2, len(data) + 2)
    filtered = apply_cdc(
        data, source_name, {source_name: watermark} if watermark else {}, force
    )
    logger.info("ATM %s: %s registros capturados por CDC", source_name, len(filtered))
    return filtered


@task(name="Extract banking API", retries=3, retry_delay_seconds=[1, 2, 4], persist_result=False)
def extract_api_file(
    file_path: str, source_name: str, watermark: str | None, force: bool
) -> pd.DataFrame:
    logger = get_run_logger()
    path = Path(file_path)
    records = json.loads(path.read_text(encoding="utf-8"))
    data = pd.DataFrame(records)
    data["_source_name"] = source_name
    data["_source_file"] = path.name
    data["_source_row"] = range(1, len(data) + 1)
    filtered = apply_cdc(
        data, source_name, {source_name: watermark} if watermark else {}, force
    )
    logger.info("API: %s registros capturados por CDC", len(filtered))
    return filtered


@task(name="Combine sources", persist_result=False)
def combine_sources(frames: list[pd.DataFrame]) -> pd.DataFrame:
    available = [frame for frame in frames if not frame.empty]
    if not available:
        return pd.DataFrame()
    return pd.concat(available, ignore_index=True)


@task(name="Land immutable raw", persist_result=False)
def land_raw(
    data: pd.DataFrame, batch_id: str, settings: dict[str, Any]
) -> str:
    path = write_dataframe(data, "raw", batch_id, "transactions_raw.csv", settings)
    return str(path)


@task(name="Persist CDC offsets", persist_result=False)
def save_cdc_offsets(scenario: str, data: pd.DataFrame) -> None:
    persist_watermarks(scenario, data)


@task(name="Row-level Data Quality", persist_result=False)
def validate_rows(data: pd.DataFrame) -> QualityResult:
    return validate_transactions(data)


@task(name="Great Expectations suite", persist_result=False)
def validate_with_gx(data: pd.DataFrame) -> dict[str, Any]:
    return run_gx_suite(data)


@task(name="Write quarantine", persist_result=False)
def quarantine_rejected(
    rejected: pd.DataFrame,
    batch_id: str,
    settings: dict[str, Any],
) -> str | None:
    if rejected.empty:
        return None
    path = write_dataframe(
        rejected, "quarantine", batch_id, "rejected_transactions.csv", settings
    )
    record_rejections(batch_id, rejected)
    return str(path)


@task(name="Quality Gate", persist_result=False)
def quality_gate(
    summary: dict[str, Any], rejection_threshold: float
) -> dict[str, Any]:
    result = evaluate_quality_gate(summary, rejection_threshold)
    if not result["passed"]:
        raise DataQualityGateError(
            "QUALITY GATE BLOQUEADO: "
            f"{result['rejected_records']} de {result['total_records']} registros "
            f"rechazados ({result['rejection_rate']:.2%})."
        )
    return result


@task(name="Transform and mask", persist_result=False)
def transform(data: pd.DataFrame, batch_id: str) -> pd.DataFrame:
    return transform_transactions(data, batch_id)


@task(name="Write validated staging", persist_result=False)
def write_validated(
    data: pd.DataFrame, batch_id: str, settings: dict[str, Any]
) -> str:
    path = write_dataframe(
        data, "validated", batch_id, "validated_transactions.csv", settings
    )
    return str(path)


@task(name="Idempotent MERGE Azure SQL", retries=2, retry_delay_seconds=[1, 2], persist_result=False)
def load_warehouse(
    data: pd.DataFrame, flow_run_id: str, pipeline_version: str
) -> dict[str, int]:
    return merge_transactions(data, flow_run_id, pipeline_version)


@task(name="Post-load financial reconciliation", persist_result=False)
def reconcile(batch_id: str, expected_count: int) -> dict[str, Any]:
    return reconcile_batch(batch_id, expected_count)


@task(name="Publication Gate SBS", persist_result=False)
def publish_report(
    batch_id: str,
    reconciliation: dict[str, Any],
) -> dict[str, Any]:
    return publish_regulatory_report(batch_id, reconciliation)


def _new_batch_id(scenario: str) -> str:
    timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")
    return f"{scenario}-{timestamp}-{uuid4().hex[:6]}"


@flow(name="FinanData TrustGate - ETL financiero", log_prints=True)
def financial_dataops_pipeline(
    scenario: str = "healthy",
    force_reprocess: bool = False,
    batch_id: str | None = None,
) -> dict[str, Any]:
    """Ejecuta el DAG completo y bloquea todo reporte que no sea confiable."""
    logger = get_run_logger()
    started_at = perf_counter()
    settings = load_settings()
    batch_id = batch_id or _new_batch_id(scenario)
    input_root = PROJECT_ROOT / "data" / "input" / scenario
    watermarks = load_watermarks(scenario)
    current_flow_run_id = str(flow_run.id or "local")

    if scenario not in {"healthy", "incident"}:
        raise ValueError("scenario debe ser 'healthy' o 'incident'")
    if not input_root.exists():
        raise FileNotFoundError(
            "No existen datos de entrada. Ejecuta: python scripts/generate_data.py"
        )

    logger.info("Iniciando lote %s con escenario %s", batch_id, scenario)
    extraction_futures = []
    for atm_path in sorted((input_root / "atm").glob("*.csv")):
        source_name = atm_path.stem
        extraction_futures.append(
            extract_atm_file.submit(
                str(atm_path),
                source_name,
                watermarks.get(source_name),
                force_reprocess,
            )
        )
    api_source = "banking_api"
    extraction_futures.append(
        extract_api_file.submit(
            str(input_root / settings["sources"]["api_file"]),
            api_source,
            watermarks.get(api_source),
            force_reprocess,
        )
    )
    extracted_frames = [future.result() for future in extraction_futures]
    data = combine_sources(extracted_frames)

    if data.empty:
        summary = {
            "total_records": 0,
            "accepted_records": 0,
            "rejected_records": 0,
            "rejection_rate": 0.0,
            "message": "Sin cambios nuevos según watermarks CDC",
        }
        record_batch(
            batch_id,
            scenario,
            "SKIPPED_NO_CHANGES",
            summary,
            settings["pipeline"]["git_commit"],
            current_flow_run_id,
        )
        return {"batch_id": batch_id, "status": "SKIPPED_NO_CHANGES", **summary}

    land_raw(data, batch_id, settings)
    save_cdc_offsets(scenario, data)
    source_volumes = {
        str(source): int(count)
        for source, count in data.groupby("_source_name").size().items()
    }
    freshness = pd.to_datetime(data["updated_at"], utc=True).max().isoformat()

    quality_future = validate_rows.submit(data)
    gx_future = validate_with_gx.submit(data)
    quality = quality_future.result()
    gx_summary = gx_future.result()
    gate_preview = evaluate_quality_gate(
        quality.summary, settings["pipeline"]["rejection_threshold"]
    )
    quality_report = {
        "batch_id": batch_id,
        "scenario": scenario,
        "observability": {
            "source_volumes": source_volumes,
            "latest_updated_at": freshness,
        },
        "row_level": gate_preview,
        "great_expectations": gx_summary,
    }
    write_json_artifact(
        quality_report, f"artifacts/quality/quality_report_{batch_id}.json"
    )
    quarantine_rejected(quality.rejected, batch_id, settings)

    try:
        gate = quality_gate(
            quality.summary, settings["pipeline"]["rejection_threshold"]
        )
    except DataQualityGateError:
        alert = {
            "severity": "CRITICAL",
            "event": "QUALITY_GATE_BLOCKED",
            "batch_id": batch_id,
            "scenario": scenario,
            "message": "Reporte SBS bloqueado; datos enviados a cuarentena.",
            "latency_seconds": round(perf_counter() - started_at, 3),
            "source_volumes": source_volumes,
            **gate_preview,
        }
        write_json_artifact(alert, f"artifacts/metrics/alert_{batch_id}.json")
        record_batch(
            batch_id,
            scenario,
            "BLOCKED_QUALITY_GATE",
            quality.summary,
            settings["pipeline"]["git_commit"],
            current_flow_run_id,
        )
        logger.critical(
            "PUBLICACIÓN SBS BLOQUEADA: rechazo %.2f%%",
            quality.summary["rejection_rate"] * 100,
        )
        raise

    transformed = transform(quality.accepted, batch_id)
    write_validated(transformed, batch_id, settings)
    merge_stats = load_warehouse(
        transformed,
        current_flow_run_id,
        settings["pipeline"]["git_commit"],
    )

    try:
        reconciliation = reconcile(batch_id, len(transformed))
    except ReconciliationError:
        record_batch(
            batch_id,
            scenario,
            "BLOCKED_RECONCILIATION",
            quality.summary,
            settings["pipeline"]["git_commit"],
            current_flow_run_id,
            merge_stats,
        )
        raise

    report = publish_report(batch_id, reconciliation)
    metrics = {
        "batch_id": batch_id,
        "status": "PUBLISHED",
        "latency_seconds": round(perf_counter() - started_at, 3),
        "source_volumes": source_volumes,
        "latest_updated_at": freshness,
        "quality_gate": gate,
        "merge": merge_stats,
        "reconciliation": reconciliation,
        "regulatory_report": report,
    }
    write_json_artifact(metrics, f"artifacts/metrics/metrics_{batch_id}.json")
    record_batch(
        batch_id,
        scenario,
        "PUBLISHED",
        quality.summary,
        settings["pipeline"]["git_commit"],
        current_flow_run_id,
        merge_stats,
    )
    logger.info("Lote %s autorizado y publicado", batch_id)
    return metrics


def main() -> None:
    parser = argparse.ArgumentParser(description="FinanData TrustGate")
    parser.add_argument("--scenario", choices=["healthy", "incident"], default="healthy")
    parser.add_argument("--force-reprocess", action="store_true")
    parser.add_argument("--batch-id")
    arguments = parser.parse_args()
    financial_dataops_pipeline(
        scenario=arguments.scenario,
        force_reprocess=arguments.force_reprocess,
        batch_id=arguments.batch_id,
    )


if __name__ == "__main__":
    main()
