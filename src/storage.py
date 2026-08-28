from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import pandas as pd

from src.settings import PROJECT_ROOT


def write_dataframe(
    data: pd.DataFrame,
    zone: str,
    batch_id: str,
    filename: str,
    settings: dict[str, Any],
) -> Path:
    """Escribe una evidencia local y opcionalmente la replica a Azure."""
    relative_root = Path(settings["storage"]["folders"][zone])
    target = PROJECT_ROOT / relative_root / batch_id / filename
    target.parent.mkdir(parents=True, exist_ok=True)
    data.to_csv(target, index=False)

    if settings["storage"]["mode"] == "azure":
        azure_zone = settings["storage"].get("azure_paths", {}).get(zone, zone)
        _upload_to_azure(
            target, Path(azure_zone) / batch_id / filename, settings
        )
    return target


def write_json_artifact(payload: dict[str, Any], relative_path: str | Path) -> Path:
    target = PROJECT_ROOT / relative_path
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False, default=str),
        encoding="utf-8",
    )
    return target


def _upload_to_azure(
    local_path: Path, blob_path: Path, settings: dict[str, Any]
) -> None:
    try:
        from azure.core.exceptions import ResourceExistsError
        from azure.storage.blob import BlobServiceClient
    except ImportError as exc:
        raise RuntimeError(
            "STORAGE_MODE=azure requiere: pip install -r requirements-azure.txt"
        ) from exc

    connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    if not connection_string:
        raise RuntimeError("Falta AZURE_STORAGE_CONNECTION_STRING en el archivo .env")

    container_name = os.getenv(
        "AZURE_STORAGE_CONTAINER", settings["storage"]["azure_container"]
    )
    service = BlobServiceClient.from_connection_string(connection_string)
    container = service.get_container_client(container_name)
    try:
        container.create_container()
    except ResourceExistsError:
        pass

    with local_path.open("rb") as stream:
        container.upload_blob(
            name=blob_path.as_posix(), data=stream, overwrite=True
        )
