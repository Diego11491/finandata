# Configuración colaborativa en Azure

## Arquitectura implementada

| Capa | Servicio o herramienta |
|---|---|
| Bronze | Azure Data Lake Storage Gen2: ATM y API sin modificar |
| Silver | Azure Data Lake: datos validados, transformados y enmascarados |
| Quarantine | Azure Data Lake: rechazados y causas de calidad |
| Gold | Azure SQL Database: hechos, lotes, rechazos y reportes SBS |
| Orquestación | Prefect 3 con servidor o Prefect Cloud |
| Data Quality | Great Expectations dentro del DAG |
| Código y CI | GitHub y GitHub Actions |

## Datos necesarios para Azure SQL

El nombre del servidor y la contraseña no son suficientes. El administrador
del equipo debe proporcionar de manera segura:

1. Nombre completo del servidor: `servidor.database.windows.net`.
2. Nombre de la base de datos.
3. Usuario SQL.
4. Contraseña SQL.
5. Acceso de firewall para la IP pública de cada integrante.

No enviar contraseñas por chat, correo grupal ni repositorio. Cada integrante
guarda las credenciales únicamente en su `.env`, archivo excluido por
`.gitignore`.

## Preparación en cada laptop Windows

1. Instalar Python 3.10-3.13.
2. Instalar Microsoft ODBC Driver 18 for SQL Server.
3. Abrir PowerShell dentro del repositorio.
4. Ejecutar:

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
Copy-Item .env.example .env
```

5. Completar `.env`:

```dotenv
WAREHOUSE_ENGINE=azure_sql
AZURE_SQL_SERVER=servidor.database.windows.net
AZURE_SQL_DATABASE=base_de_datos
AZURE_SQL_USERNAME=usuario_sql
AZURE_SQL_PASSWORD=contraseña
AZURE_SQL_DRIVER=ODBC Driver 18 for SQL Server
AZURE_SQL_SCHEMA=finandata
```

6. Verificar conexión:

```powershell
python scripts/test_azure_connection.py
```

El script crea, si tiene permisos, el esquema `finandata` y estas tablas:

- `finandata.fact_transactions`
- `finandata.etl_batches`
- `finandata.dq_rejections`
- `finandata.regulatory_reports`
- `finandata.transaction_audit_history`

## Firewall de Azure SQL

Si aparece un mensaje que indica que la dirección IP no tiene acceso:

1. Abrir el servidor SQL en Azure Portal.
2. Ir a **Redes / Networking**.
3. Agregar la dirección IPv4 pública del cliente.
4. Guardar y esperar unos minutos.
5. Repetir `python scripts/test_azure_connection.py`.

Evitar la regla `0.0.0.0 - 255.255.255.255`. Agregar solamente las IP de los
integrantes. Si su proveedor cambia la IP, deberán actualizar la regla.

## Ejecución con Prefect

Terminal 1:

```powershell
.\.venv\Scripts\Activate.ps1
prefect server start
```

Terminal 2:

```powershell
.\.venv\Scripts\Activate.ps1
$env:PREFECT_API_URL="http://127.0.0.1:4200/api"
pytest
python scripts/run_demo.py
python scripts/inspect_warehouse.py
```

Abrir `http://127.0.0.1:4200`. Se deben visualizar dos ejecuciones completadas
y una ejecución fallida intencionalmente por el Quality Gate.

## Data Lake opcional en la primera prueba

Azure SQL es obligatorio en esta versión. Para replicar también las capas
Medallion en Azure Data Lake:

```powershell
pip install -r requirements-azure.txt
```

Configurar:

```dotenv
STORAGE_MODE=azure
AZURE_STORAGE_CONNECTION_STRING=cadena_segura
AZURE_STORAGE_CONTAINER=finandata
```

El flujo escribirá:

```text
bronze/<batch_id>/transactions_raw.csv
silver/<batch_id>/validated_transactions.csv
quarantine/<batch_id>/rejected_transactions.csv
```

## Colaboración

- El código se comparte mediante GitHub, nunca copiando carpetas manualmente.
- Azure se comparte mediante IAM en el Resource Group.
- Los compañeros reciben `Contributor`, no `Owner`, salvo una decisión expresa.
- El acceso de IAM administra recursos; el acceso a datos de Azure SQL se
  controla adicionalmente con usuarios de base de datos.
- Prefect local solo es visible en la laptop que lo ejecuta. Para un tablero
  común puede utilizarse Prefect Cloud.

## Seguridad y costos

- Mantener `.env` fuera de Git.
- Cambiar la contraseña si fue enviada por un medio inseguro.
- Preferir usuarios individuales en lugar de una sola cuenta compartida.
- Configurar presupuesto y alertas de costo.
- Mantener el contenedor privado y TLS habilitado.
- El esquema `finandata` debe ser exclusivo de este laboratorio. Al iniciar
  `run_demo.py`, el script vacía sus cinco tablas para garantizar un resultado
  determinista. El reset incluye el historial únicamente porque es una demo;
  no se usaría en producción y no afecta tablas fuera de `finandata`.
- La primera ejecución registra 300 eventos `INSERT`; la segunda, 300 eventos
  `REPROCESSED` con el mismo hash; el incidente registra cero cambios en Gold.
