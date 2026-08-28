# Auditoría y trazabilidad bancaria

## Objetivo

La capa Gold no debe mostrar únicamente el estado actual de una transacción.
También debe permitir responder: cuándo llegó, de qué archivo y fila provino,
qué lote y ejecución la procesó, si cambió, qué campos cambiaron, con qué
versión del pipeline y por qué un lote fue bloqueado.

## Modelo implementado

### Estado actual: `finandata.fact_transactions`

| Columna | Uso de auditoría |
|---|---|
| `created_at` | Primera incorporación a Gold |
| `last_modified_at` | Último cambio real del contenido de negocio |
| `processed_at` | Último procesamiento, incluso si el contenido era idéntico |
| `created_batch_id` | Lote que creó la fila |
| `last_modified_batch_id` | Lote que realizó el último cambio real |
| `batch_id` | Último lote que procesó la fila |
| `record_hash` | Huella SHA-256 del contenido de negocio enmascarado |
| `source_file`, `source_row` | Linaje hasta el origen |

### Historial: `finandata.transaction_audit_history`

La tabla es append-only para el pipeline: cada procesamiento agrega un evento
y nunca actualiza eventos anteriores.

| Evento | Significado |
|---|---|
| `INSERT` | La transacción no existía y fue incorporada por primera vez |
| `UPDATE` | El hash cambió y se identificaron campos de negocio diferentes |
| `REPROCESSED` | La transacción ya existía con el mismo hash; no hubo cambio real |

Cada evento conserva hashes anterior/nuevo, campos modificados, estado antes y
después, lote, `flow_run_id`, versión Git, archivo, fila y fecha UTC. Los JSON
solo contienen información enmascarada o hasheada, nunca documento, tarjeta o
IBAN completos.

## Actualización de una instalación existente

1. Guarda una copia privada de tu `.env`; no la envíes ni la incluyas en el ZIP.
2. Abre el proyecto actualizado y coloca allí tu `.env`.
3. Activa el entorno virtual.
4. Ejecuta:

```powershell
python scripts\test_azure_connection.py
```

Este comando migra sin eliminar la base existente:

- agrega las columnas de auditoría que falten;
- completa metadatos para registros anteriores;
- agrega `created_at` a los lotes;
- crea `transaction_audit_history` y sus índices.

El mensaje esperado es:

```text
CONEXIÓN AZURE SQL CORRECTA
Esquema finandata y cinco tablas verificadas, incluida auditoría.
```

## Validación completa

```powershell
pytest -q
python scripts\run_demo.py
python scripts\inspect_warehouse.py
```

La demo debe producir:

- 300 eventos `INSERT` para `DEMO-HEALTHY-001`;
- 300 eventos `REPROCESSED` para `DEMO-HEALTHY-002`;
- cero cambios de negocio en el reprocesamiento;
- cero eventos Gold para `DEMO-INCIDENT-15PCT`;
- 45 rechazos y ningún reporte SBS para el incidente.

## Evidencias en SSMS

1. Conéctate al servidor y selecciona `FinanDataDW2`.
2. Expande **Databases → FinanDataDW2 → Tables**.
3. Pulsa **Refresh** si todavía aparecen cuatro tablas.
4. Abre `sql/audit_queries.sql`.
5. Ejecuta cada bloque por separado para obtener capturas limpias.

La evidencia principal es una transacción con dos eventos:

```text
INSERT       → ingresó por primera vez
REPROCESSED  → se volvió a procesar, pero el hash no cambió
```

Y para el lote crítico:

```text
audit_events_from_blocked_batch = 0
```

Eso demuestra que detectar una falla no fue solo informativo: el Quality Gate
impidió físicamente que el incidente modificara Gold.

## Explicación para la exposición

> Añadimos un hash SHA-256 para distinguir un cambio real de un simple
> reprocesamiento. La tabla Gold conserva cuándo se creó y cuándo cambió por
> última vez, mientras un historial append-only registra quién —mediante el
> flow run—, qué versión del pipeline, qué lote y qué campos intervinieron. Por
> eso podemos demostrar que el lote con 15% de errores dejó evidencia en
> cuarentena, pero produjo cero modificaciones en Gold y cero reportes SBS.

## Alcance y control de la demo

`scripts/run_demo.py` utiliza `reset_demo.py` para vaciar exclusivamente las
cinco tablas del esquema de laboratorio y producir resultados deterministas.
Ese reset es una utilidad de desarrollo; no formaría parte de una operación
productiva. En producción, el historial sería inmutable y la depuración se
haría mediante políticas de retención aprobadas, no mediante `DELETE`.
