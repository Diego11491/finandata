"""Limpia salidas locales y filas del esquema Azure SQL exclusivo del proyecto."""

from __future__ import annotations

import shutil
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def reset_demo() -> None:
    for relative in [
        "data/lake/bronze",
        "data/lake/silver",
        "data/lake/quarantine",
        "artifacts/quality",
        "artifacts/metrics",
    ]:
        target = ROOT / relative
        target.mkdir(parents=True, exist_ok=True)
        for child in target.iterdir():
            if child.name == ".gitkeep":
                continue
            if child.is_dir():
                shutil.rmtree(child)
            else:
                child.unlink()

    for state_file in (ROOT / "state").glob("watermarks_*.json"):
        state_file.unlink()

    from src.warehouse import reset_demo_data

    reset_demo_data()


if __name__ == "__main__":
    reset_demo()
    print("Demo local y tablas del esquema finandata reiniciadas.")
