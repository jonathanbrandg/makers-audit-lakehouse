# Prueba de caida: el core .NET sigue operando con el pipeline apagado y, al volver,
# no se pierde ni se duplica ningun evento. Deja la evidencia en evidencia\prueba-caida-<fecha>.txt
# Uso (desde la raiz del repo, con el stack levantado):
#   powershell -ExecutionPolicy Bypass -File .\scripts\prueba-caida.ps1 [-Pagos 3000]
param([int]$Pagos = 3000)

$ts = Get-Date -Format 'yyyyMMdd-HHmmss'
New-Item -ItemType Directory -Force -Path evidencia | Out-Null
$log = "evidencia\prueba-caida-$ts.txt"
# Registro propio (no Start-Transcript: su encabezado incluye usuario y nombre de la maquina)
function Write-Log([string]$Text, [string]$Color = 'Gray') {
    Write-Host $Text -ForegroundColor $Color
    Add-Content -Path $log -Value $Text -Encoding UTF8
}

$pw = ((Get-Content .env | Where-Object { $_ -like 'MSSQL_SA_PASSWORD=*' }) -replace '^MSSQL_SA_PASSWORD=', '')

function Write-Step([string]$Text) {
    Write-Log ""
    Write-Log ("[{0}] {1}" -f (Get-Date -Format 'yyyy-MM-dd HH:mm:ss'), $Text) 'Cyan'
}
function Write-Info([string]$Text) { Write-Log "           $Text" }

function Get-SqlCount {
    # Todo evento escrito: lo que sigue en la outbox + lo ya capturado por CDC (aunque la retencion lo haya purgado)
    $q = 'SET NOCOUNT ON; SELECT COUNT(*) FROM (SELECT EventId FROM dbo.AuditOutbox UNION SELECT EventId FROM cdc.dbo_AuditOutbox_CT WHERE [__$operation]=2) s'
    [int]((docker compose exec -T sqlserver /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P $pw -C -d AuditDb -h -1 -W -Q $q | Where-Object { $_.Trim() -ne '' }) | Select-Object -First 1)
}
function Get-LakeCount {
    $out = docker compose run --rm --no-deps spark-inspect /opt/spark/bin/spark-sql -S -e "SELECT COUNT(DISTINCT event_id) FROM lake.audit.events_bronze" 2>$null
    [int](($out | Where-Object { $_ -match '^\d+$' }) | Select-Object -Last 1)
}
function Write-Counts([string]$Moment) {
    $sql = Get-SqlCount; $lake = Get-LakeCount
    Write-Info ("{0}: SQL Server = {1} eventos | lakehouse = {2} | pendientes de llegar = {3}" -f $Moment, $sql, $lake, ($sql - $lake))
    return @{ Sql = $sql; Lake = $lake }
}

Write-Log "PRUEBA DE CAIDA DEL PIPELINE (evidencia en $log)" 'Cyan'
$pipelineDown = $false
try {
    Write-Step "PASO 0 - Estado inicial (contando eventos, Spark tarda unos segundos)"
    $c0 = Write-Counts "Antes"

    Write-Step "PASO 1 - Se apagan la captura (Debezium) y la ingesta (Spark)"
    docker compose stop connect spark-ingest 2>$null | Out-Null
    $pipelineDown = $true
    docker compose ps -a connect spark-ingest --format "{{.Service}}: {{.State}}" | ForEach-Object { Write-Info $_ }

    Write-Step "PASO 2 - El core .NET sigue operando: $Pagos transacciones de pago con el pipeline caido"
    $sim = Invoke-RestMethod -Method Post -TimeoutSec 600 -Uri "http://localhost:5080/api/simulate?accounts=20&payments=$Pagos"
    Write-Info ("Respuesta del core: {0} cuentas, {1} pagos, {2} reversos, {3} conflictos de concurrencia, en {4} ms" -f `
        $sim.accounts, $sim.payments, $sim.reversals, $sim.conflicts, $sim.elapsedMs)
    Write-Info "El core no depende del pipeline: escribe negocio + outbox en SQL Server y responde."

    Write-Step "PASO 3 - Con el pipeline caido, los eventos esperan en SQL Server (no se pierden)"
    $c1 = Write-Counts "Durante la caida"
    Write-Info ("Nuevos en SQL Server: {0} | nuevos en el lakehouse: {1}" -f ($c1.Sql - $c0.Sql), ($c1.Lake - $c0.Lake))

    Write-Step "PASO 4 - Se levanta el pipeline: Debezium retoma desde su ultimo LSN y Spark desde su checkpoint"
    docker compose start connect spark-ingest 2>$null | Out-Null
    $pipelineDown = $false
    $t0 = Get-Date

    Write-Step "PASO 5 - Reconciliacion evento por evento (SQL Server vs lakehouse) hasta quedar al dia"
    $env:E2E_SKIP_TRAFFIC = 'true'
    docker compose up -d --no-deps --force-recreate e2e 2>$null | Out-Null
    docker compose wait e2e | Out-Null
    Remove-Item Env:\E2E_SKIP_TRAFFIC
    Write-Info ("Recuperacion completa en {0:N0} s desde que se levanto el pipeline" -f ((Get-Date) - $t0).TotalSeconds)

    Write-Step "PASO 6 - dbt reconstruye silver y gold y valida las capas"
    docker compose up -d --no-deps --force-recreate dbt 2>$null | Out-Null
    docker compose wait dbt | Out-Null

    Write-Step "RESULTADO"
    # 6>&1 captura el Write-Host del otro script para guardarlo tambien en la evidencia
    $lines = & "$PSScriptRoot\resultado.ps1" 6>&1
    $code = $LASTEXITCODE
    $lines | ForEach-Object { Write-Log "$_" }
}
finally {
    if ($pipelineDown) {
        Write-Log "Interrumpido: se vuelve a levantar el pipeline" 'Yellow'
        docker compose start connect spark-ingest 2>$null | Out-Null
    }
    Write-Host ""
    Write-Host "Evidencia guardada en: $log"
}
exit $code
