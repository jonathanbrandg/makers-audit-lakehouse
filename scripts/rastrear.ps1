# Sigue los eventos de auditoria de una entidad (cuenta o pago) por cada etapa del pipeline.
# Uso (desde la raiz del repo), con el id que devolvio la API:
#   powershell -ExecutionPolicy Bypass -File .\scripts\rastrear.ps1 <id>
param([Parameter(Mandatory = $true)][string]$Id)

$id = $Id.Trim().ToLower()
if ($id -notmatch '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$') {
    Write-Host "El id debe ser un GUID, por ejemplo 3f2a1c4e-0000-0000-0000-000000000000" -ForegroundColor Red
    exit 2
}
$pw = ((Get-Content .env | Where-Object { $_ -like 'MSSQL_SA_PASSWORD=*' }) -replace '^MSSQL_SA_PASSWORD=', '')

Write-Host ""
Write-Host "RASTREO DE LA ENTIDAD $id" -ForegroundColor Cyan

# ---------------------------------------------------------------- 1. SQL Server
$sql = 'SET NOCOUNT ON; ' +
  'SELECT LOWER(CONVERT(varchar(36),o.EventId))+'' | ''+o.EntityType+'' ''+o.Action+'' | ''+CONVERT(varchar(19),o.OccurredAt,120)+'' | ''+' +
  'CASE WHEN ct.EventId IS NULL THEN ''en outbox, CDC todavia no lo capturo'' ELSE ''en outbox y capturado por CDC'' END ' +
  'FROM dbo.AuditOutbox o LEFT JOIN cdc.dbo_AuditOutbox_CT ct ON ct.EventId=o.EventId AND ct.[__$operation]=2 ' +
  "WHERE LOWER(o.EntityId)='$id' " +
  'UNION ALL ' +
  'SELECT LOWER(CONVERT(varchar(36),ct.EventId))+'' | ''+ct.EntityType+'' ''+ct.Action+'' | ''+CONVERT(varchar(19),ct.OccurredAt,120)+'' | purgado de la outbox por retencion (copia en CDC)'' ' +
  'FROM cdc.dbo_AuditOutbox_CT ct WHERE ct.[__$operation]=2 ' +
  "AND LOWER(ct.EntityId)='$id' AND NOT EXISTS (SELECT 1 FROM dbo.AuditOutbox o WHERE o.EventId=ct.EventId)"

$sqlRows = @(docker compose exec -T sqlserver /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P $pw -C -d AuditDb -h -1 -W -Q $sql |
    Where-Object { $_.Trim() -ne '' })

Write-Host ""
Write-Host "1) SQL Server - evento guardado junto con la operacion de negocio"
if ($sqlRows.Count -eq 0) {
    Write-Host "   [X] No hay eventos para ese id. Revisa que sea el id que devolvio la API." -ForegroundColor Red
    exit 1
}
Write-Host "   [OK] $($sqlRows.Count) evento(s)" -ForegroundColor Green
$sqlRows | ForEach-Object { Write-Host "        $_" }

# ---------------------------------------------------------------- 2 y 3. Lakehouse
Write-Host ""
Write-Host "   Consultando el lakehouse (Spark tarda unos segundos en arrancar)..."
$q = "SELECT '##BRONZE'; " +
     "SELECT event_id, entity_type, action, kafka_partition, kafka_offset, CAST(ingested_at AS STRING) FROM lake.audit.events_bronze WHERE lower(entity_id)='$id' ORDER BY event_ts; " +
     "SELECT '##SILVER'; " +
     "SELECT event_id, entity_type, action, CAST(dbt_loaded_at AS STRING) FROM lake.audit_silver.audit_events WHERE lower(entity_id)='$id' ORDER BY event_ts;"
$out = @(docker compose run --rm --no-deps spark-inspect /opt/spark/bin/spark-sql -S -e $q 2>$null)

$bronze = @(); $silver = @(); $section = ''
foreach ($line in $out) {
    if ($line -eq '##BRONZE') { $section = 'b'; continue }
    if ($line -eq '##SILVER') { $section = 's'; continue }
    if ($line.Trim() -eq '') { continue }
    if ($section -eq 'b') { $bronze += $line }
    if ($section -eq 's') { $silver += $line }
}

Write-Host ""
Write-Host "2) Kafka -> Spark -> Iceberg bronze (automatico, cada ~30 s)"
if ($bronze.Count -ge $sqlRows.Count) {
    Write-Host "   [OK] $($bronze.Count) de $($sqlRows.Count) evento(s) en el lakehouse" -ForegroundColor Green
} else {
    Write-Host "   [..] $($bronze.Count) de $($sqlRows.Count) evento(s). Todavia en camino: espera 30 s y volve a correr el script." -ForegroundColor Yellow
}
Write-Host "        event_id | tipo | accion | particion Kafka | offset Kafka | ingresado"
$bronze | ForEach-Object { Write-Host ("        " + ($_ -replace "`t", ' | ')) }

Write-Host ""
Write-Host "3) dbt silver (se actualiza al correr dbt, no es continuo)"
if ($silver.Count -ge $sqlRows.Count) {
    Write-Host "   [OK] $($silver.Count) de $($sqlRows.Count) evento(s) en silver" -ForegroundColor Green
} else {
    Write-Host "   [..] $($silver.Count) de $($sqlRows.Count) evento(s). Para actualizar: docker compose run --rm --no-deps dbt" -ForegroundColor Yellow
}
$silver | ForEach-Object { Write-Host ("        " + ($_ -replace "`t", ' | ')) }
Write-Host ""
