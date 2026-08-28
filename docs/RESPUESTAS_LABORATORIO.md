# Desarrollo del laboratorio - FinanData TrustGate

## Parte 1. Análisis

### Problema en términos de DataOps

FinanData Perú carecía de un ciclo DataOps capaz de convertir las validaciones en controles ejecutables. El pipeline detectó que 15% de las transacciones era inválido, pero continuó con la transformación, carga y generación del reporte regulatorio. Por tanto, existía observación sin prevención, además de poca trazabilidad y una automatización insegura.

### Por qué falló el control de calidad

La validación funcionaba como una tarea informativa y no como una compuerta. No había tolerancias según severidad, cuarentena, condición de fallo ni dependencia entre la aprobación de calidad y la publicación. Tampoco se ejecutaba una conciliación financiera postcarga.

### Etapa donde ocurrió el error

El fallo primario estuvo en **Validación/Orquestación**, porque un lote rechazado pudo continuar. El fallo secundario estuvo antes del **reporte SBS**, porque no existía un Publication Gate que comprobara calidad y conciliación.

## Parte 2. Diseño

### Patrón seleccionado

Se utiliza **ETL**. Los datos originales aterrizan primero en una zona raw inmutable para auditoría, pero la carga al Warehouse ocurre solo después de validar, calcular comisiones, enriquecer el riesgo y enmascarar datos sensibles. Un ELT puro trasladaría datos no confiables a la capa analítica antes de aplicar controles, elevando el riesgo regulatorio.

### DAG

1. Extracción paralela de archivos ATM por sucursal.
2. Extracción de la API bancaria.
3. Filtro CDC mediante `updated_at` y watermark por fuente.
4. Aterrizaje raw inmutable.
5. Validación de esquema y reglas financieras.
6. Separación aceptados/rechazados y cuarentena.
7. Quality Gate.
8. Comisión, score de riesgo y enmascaramiento.
9. Staging validado.
10. `MERGE` idempotente al Warehouse.
11. Conteo y conciliación `Débito = Crédito`.
12. Publication Gate.
13. Reporte regulatorio y métricas.

### Reglas propuestas

| Regla | Severidad | Acción |
|---|---|---|
| ID obligatorio y con formato válido | Crítica | Cuarentena y bloqueo |
| ID único dentro del lote | Crítica | Cuarentena y bloqueo |
| Monto no negativo | Crítica | Cuarentena y bloqueo |
| IBAN de origen/destino válido | Crítica | Cuarentena y bloqueo |
| Cuenta origen distinta de destino | Crítica | Cuarentena y bloqueo |
| Moneda PEN o USD | Crítica | Cuarentena y bloqueo |
| Fechas válidas | Crítica | Cuarentena y bloqueo |
| CDC `I` o `U` | Crítica | Cuarentena y bloqueo |
| Rechazo total ≤ 1% | SLA configurable | Si lo supera, bloquear |
| Volumen anómalo | Advertencia | Revisión operativa |

El umbral de 1% es un parámetro de demostración. En producción se aprobaría con Riesgos, Data Governance y Cumplimiento a partir de históricos y materialidad regulatoria. Las reglas críticas conservan tolerancia cero.

## Parte 3. Implementación conceptual

```python
@flow(name="ETL financiero")
def pipeline(scenario):
    atm = extract_atm.map(branches)       # paralelo por sucursal
    api = extract_api.submit()            # 3 reintentos técnicos
    raw = land_raw(combine(atm, api))

    row_quality = validate_rows.submit(raw)
    gx_quality = validate_with_gx.submit(raw)
    accepted, rejected, metrics = row_quality.result()
    quarantine(rejected)
    quality_gate(metrics)                 # falla si el lote no es confiable

    trusted = transform_and_mask(accepted)
    merge_warehouse(trusted)              # idempotente
    reconciliation = reconcile(trusted)   # débito = crédito y conteos
    publication_gate(reconciliation)
    generate_sbs_report()
```

Los reintentos se asignan solo a operaciones transitorias. Validación, Quality Gate y conciliación no reintentan errores de negocio.

## Parte 4. DataOps

### Testing automatizado

- `pytest` valida datos sanos, incidente exacto del 15%, masking, doble partida y `MERGE` idempotente.
- Great Expectations valida unicidad, obligatoriedad, monto, IBAN y moneda.
- GitHub Actions ejecuta los tests ante `push` y `pull_request`.
- Las pruebas postcarga verifican conteos y sumas débito/crédito.

### Métricas

- Volumen extraído, aceptado y rechazado.
- Porcentaje de rechazo por lote y regla.
- Latencia total y por tarea en Prefect.
- Insertados y actualizados por `MERGE`.
- Diferencia débito-crédito.
- Estado de Quality Gate y Publication Gate.
- Antigüedad del último `updated_at`.

### Alertas

- `QUALITY_GATE_BLOCKED` por regla crítica o rechazo superior al umbral.
- Fallo de API o archivo tras agotar reintentos.
- Diferencia contable postcarga.
- Volumen o latencia fuera del SLA.
- Intento de publicación sin autorización.

La evidencia del MVP se escribe en JSON. En cloud, Prefect puede enviar notificaciones por Teams, Slack o correo y Azure Monitor centraliza métricas y logs.

## Parte 5. Mejora continua

### Backfill

El flujo se parametriza por periodo, sucursal y fuente. Lee las particiones raw históricas, genera un nuevo `batch_id` de backfill y conserva la relación con el periodo reprocesado. La publicación del periodo permanece bloqueada hasta volver a conciliar.

### Reprocesamiento

Los registros corregidos se recuperan de cuarentena y vuelven a pasar por todas las reglas. `transaction_id` funciona como clave de negocio en `MERGE`, por lo que un reproceso actualiza registros existentes y no crea duplicados.

### Trazabilidad

Cada fila conserva fechas de creación, modificación y procesamiento, lote creador y modificador, `record_hash`, archivo y número de fila. La tabla append-only `transaction_audit_history` registra eventos `INSERT`, `UPDATE` y `REPROCESSED`, hashes anterior/nuevo, campos modificados, estado antes/después, ejecución de Prefect y versión Git. Junto con `etl_batches`, `dq_rejections` y `regulatory_reports`, permite reconstruir qué ocurrió y demostrar que un lote bloqueado no modificó Gold ni obtuvo autorización regulatoria.

## Respuesta al incidente

En la reproducción del incidente se procesan 300 registros y exactamente 45 son rechazados. El pipeline conserva el raw, registra los 45 rechazos, genera una alerta crítica, marca el lote como `BLOCKED_QUALITY_GATE` y deja en cero los reportes SBS asociados al lote. De esta manera, el pipeline puede completar actividades de diagnóstico y cuarentena sin permitir la propagación del dato defectuoso.
