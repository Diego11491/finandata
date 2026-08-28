from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from src.settings import PROJECT_ROOT


def _watermark_path(scenario: str) -> Path:
    return PROJECT_ROOT / "state" / f"watermarks_{scenario}.json"


def load_watermarks(scenario: str) -> dict[str, str]:
    path = _watermark_path(scenario)
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def apply_cdc(
    data: pd.DataFrame, source_name: str, watermarks: dict[str, str], force: bool
) -> pd.DataFrame:
    if force or source_name not in watermarks:
        return data.copy()
    updated_at = pd.to_datetime(data["updated_at"], errors="coerce", utc=True)
    previous = pd.to_datetime(watermarks[source_name], utc=True)
    return data.loc[updated_at > previous].copy()


def persist_watermarks(scenario: str, data: pd.DataFrame) -> None:
    path = _watermark_path(scenario)
    watermarks = load_watermarks(scenario)
    for source_name, frame in data.groupby("_source_name"):
        maximum = pd.to_datetime(frame["updated_at"], errors="coerce", utc=True).max()
        if not pd.isna(maximum):
            watermarks[str(source_name)] = maximum.isoformat()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(watermarks, indent=2), encoding="utf-8")


def reset_watermarks(scenario: str) -> None:
    path = _watermark_path(scenario)
    if path.exists():
        path.unlink()

