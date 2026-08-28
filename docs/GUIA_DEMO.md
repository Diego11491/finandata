# Guía de demostración y capturas

## Preparación

```powershell
.\.venv\Scripts\Activate.ps1
python scripts/test_azure_connection.py
pytest
python scripts/run_demo.py
python scripts/inspect_warehouse.py
```

## Evidencias que deben capturarse

1. **DAG de Prefect:** tareas ATM ejecutándose en paralelo y luego convergiendo en Data Quality.
2. **Ejecución sana:** estado `Completed` y log “autorizado y publicado”.
3. **Idempotencia:** 0 insertados, 300 coincidencias y 300 eventos `REPROCESSED`.
4. **Incidente:** tarea `Quality Gate` fallida y mensaje de publicación SBS bloqueada.
5. **Cuarentena:** CSV con 45 filas y columna `_rejection_reasons`.
6. **Reporte de calidad:** JSON con `rejection_rate: 0.15`.
7. **Azure SQL:** tabla `finandata.etl_batches` y ausencia de reporte para `DEMO-INCIDENT-15PCT`.
8. **Testing:** resultado verde de `pytest`.
9. **Auditoría:** 300 `INSERT`, 300 `REPROCESSED` y cero eventos del lote bloqueado.
10. **Linaje:** una transacción con `flow_run_id`, versión Git, archivo/fila y hashes anterior/nuevo.

Nota para interpretar Great Expectations: en una duplicidad existen dos filas involucradas. Por eso su expectativa de unicidad puede reportar 20 valores inesperados para 10 pares, mientras nuestra cuarentena conserva la primera ocurrencia y rechaza únicamente las 10 repeticiones.

## Orden narrativo para la exposición

1. Mostrar el riesgo: detectar 15% de errores no evitó el reporte incorrecto.
2. Explicar ETL y las capas Bronze, Silver, Quarantine y Gold.
3. Recorrer el DAG hasta el Quality Gate.
4. Ejecutar el lote sano.
5. Reejecutarlo y mostrar que `MERGE` no duplica: conserva el hash y registra
   `REPROCESSED`, no un cambio de negocio.
6. Ejecutar el incidente y mostrar cuarentena, alerta y bloqueo.
7. Mostrar que el incidente produjo cero eventos en Gold.
8. Cerrar con trazabilidad, backfill y colaboración sobre Azure SQL.

## Frase central

> La calidad de datos no es solamente un reporte: es una condición técnica que autoriza o impide que la información continúe hacia el regulador.
