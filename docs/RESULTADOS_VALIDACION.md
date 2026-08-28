# Resultados de la validación ejecutada

Fecha de validación de auditoría: 27 de agosto de 2026.

> La línea base validó los tres escenarios. La extensión de auditoría fue
> validada mediante nueve pruebas automáticas. Después de migrar Azure SQL,
> `scripts/run_demo.py` comprueba también los conteos del historial cloud.

| Lote | Estado | Calidad | Gold/Auditoría | Reporte SBS |
|---|---|---|---|---|
| DEMO-HEALTHY-001 | PUBLISHED | 300/300 aceptados | 300 INSERT esperados | Autorizado |
| DEMO-HEALTHY-002 | PUBLISHED | 300/300 aceptados | 300 REPROCESSED esperados | Autorizado |
| DEMO-INCIDENT-15PCT | BLOCKED_QUALITY_GATE | 255 aceptados, 45 rechazados | 0 eventos esperados | Bloqueado |

## Distribución de los 45 rechazos

| Causa | Filas rechazadas |
|---|---:|
| Monto negativo o inválido | 15 |
| IBAN de origen ausente/inválido | 10 |
| ID de transacción repetido | 10 |
| Misma cuenta para débito y crédito | 10 |

## Controles verificados

- Extracción concurrente de tres sucursales ATM y una API.
- Raw inmutable y watermarks CDC por fuente.
- Seis Expectations automáticas y validación detallada por fila.
- Enmascaramiento de tarjeta, documento e IBAN antes del Warehouse.
- `MERGE` idempotente comprobado mediante reproceso.
- Conteo postcarga consistente y `Débito = Crédito`.
- Cuarentena y alerta del lote crítico.
- Ausencia total de reporte regulatorio para el incidente.
- Hash estable ante reproceso y diferente cuando cambia el negocio.
- Clasificación `INSERT`, `UPDATE` y `REPROCESSED` verificada.
- Ocho pruebas automáticas aprobadas.

Los resultados cloud de auditoría deben capturarse después de ejecutar la nueva
versión contra Azure SQL; no se inventan latencias ni se presentan como SLA.
