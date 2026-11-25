# Muestra el resultado de la prueba end-to-end en lenguaje simple.
# Uso (desde la raiz del repo):
#   powershell -ExecutionPolicy Bypass -File .\scripts\resultado.ps1              # ultimo resultado
#   powershell -ExecutionPolicy Bypass -File .\scripts\resultado.ps1 -Actualizar  # volver a verificar ahora
# -Actualizar no genera trafico: vuelve a contar lo que hay (incluye lo creado a mano) y corre dbt.
param([switch]$Actualizar)

if ($Actualizar) {
    Write-Host "Verificando de nuevo con los datos actuales (1-2 minutos)..." -ForegroundColor Cyan
    $env:E2E_SKIP_TRAFFIC = 'true'
    docker compose up -d --no-deps --force-recreate e2e 2>$null | Out-Null
    docker compose wait e2e | Out-Null
    Remove-Item Env:\E2E_SKIP_TRAFFIC
    docker compose up -d --no-deps --force-recreate dbt 2>$null | Out-Null
    docker compose wait dbt | Out-Null
}

function Get-ServiceStatus([string]$Service) {
    $id = docker compose ps -a -q $Service
    if (-not $id) { return @{ State = 'missing'; Code = $null } }
    $parts = (docker inspect -f '{{.State.Status}} {{.State.ExitCode}}' $id) -split ' '
    return @{ State = $parts[0]; Code = [int]$parts[1] }
}

function Write-Verdict([string]$Label, $Status) {
    if ($Status.State -eq 'missing')  { Write-Host "  [--] $Label : no se ejecuto (correr: docker compose up -d --build)" -ForegroundColor Yellow; return $false }
    if ($Status.State -ne 'exited')   { Write-Host "  [..] $Label : todavia en ejecucion, espera y volve a correr este script" -ForegroundColor Yellow; return $false }
    if ($Status.Code -eq 0)           { Write-Host "  [OK] $Label" -ForegroundColor Green; return $true }
    Write-Host "  [X]  $Label : FALLO (codigo $($Status.Code)). Detalle: docker compose logs $($Label.Split(' ')[0].ToLower())" -ForegroundColor Red
    return $false
}

Write-Host ""
Write-Host "RESULTADO DE LA PRUEBA DE CONCEPTO" -ForegroundColor Cyan
Write-Host ""

$e2e = Get-ServiceStatus 'e2e'
$dbt = Get-ServiceStatus 'dbt'

# Resumen que imprime la verificacion (lineas marcadas con '>> ')
docker compose logs --no-log-prefix e2e 2>$null |
    Where-Object { $_ -like '>> *' } |
    ForEach-Object { Write-Host ('  ' + $_.Substring(3)) }

# Tests de calidad de dbt
$done = docker compose logs --no-log-prefix dbt 2>$null | Where-Object { $_ -match 'Done\. PASS=(\d+).*ERROR=(\d+).*TOTAL=(\d+)' } | Select-Object -Last 1
if ($done -and $done -match 'PASS=(\d+).*ERROR=(\d+).*TOTAL=(\d+)') {
    Write-Host ""
    Write-Host "  Tests de calidad de datos (dbt): $($Matches[1]) de $($Matches[3]) aprobados, $($Matches[2]) con error"
}

Write-Host ""
$ok1 = Write-Verdict 'E2E  - SQL Server llega completo al lakehouse, sin perdidas ni duplicados' $e2e
$ok2 = Write-Verdict 'dbt  - capas silver y gold construidas y validadas' $dbt
Write-Host ""
if ($ok1 -and $ok2) {
    Write-Host "VEREDICTO: APROBADA" -ForegroundColor Green
    exit 0
}
Write-Host "VEREDICTO: NO APROBADA (ver detalle arriba)" -ForegroundColor Red
exit 1
