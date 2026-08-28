"""Comprueba conexión, esquema y sintaxis del MERGE directamente en Azure SQL."""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.warehouse import test_connection


if __name__ == "__main__":
    try:
        result = test_connection()
    except Exception as exc:
        raise SystemExit(f"VALIDACIÓN AZURE SQL FALLIDA\n{exc}") from exc
    print("CONEXIÓN AZURE SQL CORRECTA")
    print(f"Servidor: {result['server']}")
    print(f"Base de datos: {result['database']}")
    print(f"Usuario: {result['user']}")
    print("Esquema finandata y cinco tablas verificadas, incluida auditoría.")
    print("MERGE idempotente validado en Azure SQL sin modificar datos.")
