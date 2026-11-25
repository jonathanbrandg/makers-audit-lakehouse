#!/usr/bin/env bash
# Prueba de caida: el core .NET sigue operando con el pipeline apagado y, al volver,
# no se pierde ni se duplica ningun evento. Deja la evidencia en evidencia/prueba-caida-<fecha>.txt
# Uso (desde la raiz del repo, con el stack levantado):  bash scripts/prueba-caida.sh [pagos]
set -u
export MSYS_NO_PATHCONV=1 # Git Bash en Windows: no reescribir rutas /opt/...

PAGOS="${1:-3000}"
mkdir -p evidencia
LOG="evidencia/prueba-caida-$(date +%Y%m%d-%H%M%S).txt"
exec > >(tee -a "$LOG") 2>&1 # todo lo que se imprime queda tambien en la evidencia

pw=$(grep '^MSSQL_SA_PASSWORD=' .env | cut -d= -f2-)

step() { echo; echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }
info() { echo "           $*"; }

sql_count() { # outbox + lo ya capturado por CDC (aunque la retencion lo haya purgado)
  docker compose exec -T sqlserver /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "$pw" -C -d AuditDb -h -1 -W \
    -Q 'SET NOCOUNT ON; SELECT COUNT(*) FROM (SELECT EventId FROM dbo.AuditOutbox UNION SELECT EventId FROM cdc.dbo_AuditOutbox_CT WHERE [__$operation]=2) s' \
    | tr -d '\r ' | grep -E '^[0-9]+$' | head -1
}
lake_count() {
  docker compose run --rm --no-deps spark-inspect /opt/spark/bin/spark-sql -S \
    -e "SELECT COUNT(DISTINCT event_id) FROM lake.audit.events_bronze" 2>/dev/null | tr -d '\r ' | grep -E '^[0-9]+$' | tail -1
}
counts() { # $1 = momento; deja SQL y LAKE en variables globales
  SQL=$(sql_count); LAKE=$(lake_count)
  info "$1: SQL Server = $SQL eventos | lakehouse = $LAKE | pendientes de llegar = $((SQL - LAKE))"
}

PIPELINE_DOWN=0
cleanup() {
  if [ "$PIPELINE_DOWN" = "1" ]; then
    echo "Interrumpido: se vuelve a levantar el pipeline"
    docker compose start connect spark-ingest >/dev/null 2>&1
  fi
}
trap cleanup EXIT

echo "PRUEBA DE CAIDA DEL PIPELINE (evidencia en $LOG)"

step "PASO 0 - Estado inicial (contando eventos, Spark tarda unos segundos)"
counts "Antes"; SQL0=$SQL; LAKE0=$LAKE

step "PASO 1 - Se apagan la captura (Debezium) y la ingesta (Spark)"
docker compose stop connect spark-ingest >/dev/null 2>&1
PIPELINE_DOWN=1
docker compose ps -a connect spark-ingest --format "{{.Service}}: {{.State}}" | while read -r l; do info "$l"; done

step "PASO 2 - El core .NET sigue operando: $PAGOS transacciones de pago con el pipeline caido"
sim=$(curl -s -X POST --max-time 600 "http://localhost:5080/api/simulate?accounts=20&payments=$PAGOS")
info "Respuesta del core: $sim"
info "El core no depende del pipeline: escribe negocio + outbox en SQL Server y responde."

step "PASO 3 - Con el pipeline caido, los eventos esperan en SQL Server (no se pierden)"
counts "Durante la caida"
info "Nuevos en SQL Server: $((SQL - SQL0)) | nuevos en el lakehouse: $((LAKE - LAKE0))"

step "PASO 4 - Se levanta el pipeline: Debezium retoma desde su ultimo LSN y Spark desde su checkpoint"
docker compose start connect spark-ingest >/dev/null 2>&1
PIPELINE_DOWN=0
t0=$(date +%s)

step "PASO 5 - Reconciliacion evento por evento (SQL Server vs lakehouse) hasta quedar al dia"
E2E_SKIP_TRAFFIC=true docker compose up -d --no-deps --force-recreate e2e >/dev/null 2>&1
docker compose wait e2e >/dev/null
info "Recuperacion completa en $(( $(date +%s) - t0 )) s desde que se levanto el pipeline"

step "PASO 6 - dbt reconstruye silver y gold y valida las capas"
docker compose up -d --no-deps --force-recreate dbt >/dev/null 2>&1
docker compose wait dbt >/dev/null

step "RESULTADO"
bash "$(dirname "$0")/resultado.sh"
code=$?
echo; echo "Evidencia guardada en: $LOG"
exit $code
