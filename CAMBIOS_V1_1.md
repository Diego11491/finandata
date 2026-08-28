# FinanData TrustGate v1.1 — Auditoría reforzada

## Corrección v1.1.2

- Azure SQL utiliza una única rama `WHEN MATCHED` en el `MERGE`, como exige
  T-SQL.
- La fecha y el lote de última modificación se actualizan con `CASE` solo si
  el hash detecta un cambio de negocio; un reproceso conserva ambos valores.
- La prueba de conexión incorpora un smoke test del `MERGE` vacío en Azure SQL
  para detectar incompatibilidades del motor antes de ejecutar la demo.

## Corrección v1.1.1

- La migración automática desde una tabla v1.0 separa la creación y el uso de
  las nuevas columnas en lotes distintos, como exige la compilación de SQL
  Server.
- Se agregó una prueba de regresión para esta ruta de actualización.

## Cambios principales

- Hash SHA-256 estable por transacción enmascarada.
- Fechas separadas de creación, cambio real y último procesamiento.
- Lote creador, último lote modificador y último lote procesado.
- Historial append-only con eventos `INSERT`, `UPDATE` y `REPROCESSED`.
- Estado antes/después, campos modificados, hashes anterior/nuevo.
- Linaje con `flow_run_id`, versión Git, archivo y fila fuente.
- Migración automática de las cuatro tablas existentes a cinco tablas.
- Índices para consultar por transacción, fecha y lote.
- Consultas seguras para SSMS sin `SELECT *`.
- Ocho pruebas automáticas aprobadas.

## Actualización rápida

1. Conserva tu `.env` privado.
2. Abre esta versión del proyecto y coloca allí tu `.env`.
3. Ejecuta:

```powershell
python scripts\test_azure_connection.py
pytest -q
python scripts\run_demo.py
python scripts\inspect_warehouse.py
```

No ejecutes manualmente `sql/migrate_audit_v1_1.sql` si el primer comando ya
terminó correctamente: ambos realizan la misma migración.
