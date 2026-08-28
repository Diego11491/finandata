from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def load_settings() -> dict[str, Any]:
    load_dotenv(PROJECT_ROOT / ".env")
    with (PROJECT_ROOT / "config" / "settings.yaml").open(encoding="utf-8") as stream:
        settings: dict[str, Any] = yaml.safe_load(stream)

    settings["storage"]["mode"] = os.getenv(
        "STORAGE_MODE", settings["storage"]["mode"]
    ).lower()
    settings["warehouse"]["engine"] = os.getenv(
        "WAREHOUSE_ENGINE", settings["warehouse"]["engine"]
    ).lower()
    settings["warehouse"]["schema"] = os.getenv(
        "AZURE_SQL_SCHEMA", settings["warehouse"]["schema"]
    )
    settings["pipeline"]["rejection_threshold"] = float(
        os.getenv(
            "REJECTION_THRESHOLD", settings["pipeline"]["rejection_threshold"]
        )
    )
    settings["pipeline"]["git_commit"] = os.getenv(
        "PIPELINE_GIT_COMMIT", "local-dev"
    )
    return settings


def project_path(relative_path: str | Path) -> Path:
    return PROJECT_ROOT / relative_path
