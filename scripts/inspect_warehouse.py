import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.warehouse import fetch_operational_snapshot


snapshot = fetch_operational_snapshot()
print("\nLOTES EN AZURE SQL")
print(snapshot["batches"].to_string(index=False))
print("\nREPORTES REGULATORIOS AUTORIZADOS")
print(snapshot["reports"].to_string(index=False))
print("\nRECHAZOS POR REGLA")
print(snapshot["rejections"].to_string(index=False))
print("\nEVENTOS DE AUDITORÍA")
print(snapshot["audit_summary"].to_string(index=False))
print("\nÚLTIMOS 20 EVENTOS DE AUDITORÍA")
print(snapshot["recent_audit"].to_string(index=False))
