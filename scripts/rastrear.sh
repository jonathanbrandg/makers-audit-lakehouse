#!/usr/bin/env bash
# Sigue los eventos de auditoria de una entidad (cuenta o pago) por cada etapa del pipeline.
# Uso (desde la raiz del repo), con el id que devolvio la API:  bash scripts/rastrear.sh <id>
set -u
export MSYS_NO_PATHCONV=1 # Git Bash en Windows: no reescribir rutas /opt/...

id=$(echo "${1:-}" | tr 'A-Z' 'a-z' | tr -d '[:space:]')
if ! [[ "$id" =~ ^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$ ]]; then
  echo "Uso: bash scripts/rastrear.sh <id>   (el id es el GUID que devolvio la API)"; exit 2
fi
pw=$(grep '^MSSQL_SA_PASSWORD=' .env | cut -d= -f2-)

echo; echo "RASTREO DE LA ENTIDAD $id"

# ---------------------------------------------------------------- 1. SQL Server
sql="SET NOCOUNT ON;
SELECT LOWER(CONVERT(varchar(36),o.EventId))+' | '+o.EntityType+' '+o.Action+' | '+CONVERT(varchar(19),o.OccurredAt,120)+' | '+
  CASE WHEN ct.EventId IS NULL THEN 'en outbox, CDC todavia no lo capturo' ELSE 'en outbox y capturado por CDC' END
FROM dbo.AuditOutbox o LEFT JOIN cdc.dbo_AuditOutbox_CT ct ON ct.EventId=o.EventId AND ct.[__\$operation]=2
WHERE LOWER(o.EntityId)='$id'
UNION ALL
SELECT LOWER(CONVERT(varchar(36),ct.EventId))+' | '+ct.EntityType+' '+ct.Action+' | '+CONVERT(varchar(19),ct.OccurredAt,120)+' | purgado de la outbox por retencion (copia en CDC)'
FROM cdc.dbo_AuditOutbox_CT ct WHERE ct.[__\$operation]=2
  AND LOWER(ct.EntityId)='$id' AND NOT EXISTS (SELECT 1 FROM dbo.AuditOutbox o WHERE o.EventId=ct.EventId)"

sql_rows=$(docker compose exec -T sqlserver /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "$pw" -C -d AuditDb -h -1 -W -Q "$sql" | sed '/^[[:space:]]*$/d')
n_sql=$(printf '%s' "$sql_rows" | grep -c .)

echo; echo "1) SQL Server - evento guardado junto con la operacion de negocio"
if [ "$n_sql" -eq 0 ]; then
  echo "   [X] No hay eventos para ese id. Revisa que sea el id que devolvio la API."; exit 1
fi
echo "   [OK] $n_sql evento(s)"
printf '%s\n' "$sql_rows" | sed 's/^/        /'

# ---------------------------------------------------------------- 2 y 3. Lakehouse
echo; echo "   Consultando el lakehouse (Spark tarda unos segundos en arrancar)..."
q="SELECT '##BRONZE';
SELECT event_id, entity_type, action, kafka_partition, kafka_offset, CAST(ingested_at AS STRING) FROM lake.audit.events_bronze WHERE lower(entity_id)='$id' ORDER BY event_ts;
SELECT '##SILVER';
SELECT event_id, entity_type, action, CAST(dbt_loaded_at AS STRING) FROM lake.audit_silver.audit_events WHERE lower(entity_id)='$id' ORDER BY event_ts;"
out=$(docker compose run --rm --no-deps spark-inspect /opt/spark/bin/spark-sql -S -e "$q" 2>/dev/null)
bronze=$(printf '%s\n' "$out" | sed -n '/^##BRONZE$/,/^##SILVER$/p' | sed '1d;$d' | sed '/^[[:space:]]*$/d')
silver=$(printf '%s\n' "$out" | sed -n '/^##SILVER$/,$p' | sed '1d' | sed '/^[[:space:]]*$/d')
n_b=$(printf '%s' "$bronze" | grep -c .); n_s=$(printf '%s' "$silver" | grep -c .)

echo; echo "2) Kafka -> Spark -> Iceberg bronze (automatico, cada ~30 s)"
if [ "$n_b" -ge "$n_sql" ]; then echo "   [OK] $n_b de $n_sql evento(s) en el lakehouse"
else echo "   [..] $n_b de $n_sql evento(s). Todavia en camino: espera 30 s y volve a correr el script."; fi
echo "        event_id | tipo | accion | particion Kafka | offset Kafka | ingresado"
[ -n "$bronze" ] && printf '%s\n' "$bronze" | sed 's/\t/ | /g; s/^/        /'

echo; echo "3) dbt silver (se actualiza al correr dbt, no es continuo)"
if [ "$n_s" -ge "$n_sql" ]; then echo "   [OK] $n_s de $n_sql evento(s) en silver"
else echo "   [..] $n_s de $n_sql evento(s). Para actualizar: docker compose run --rm --no-deps dbt"; fi
[ -n "$silver" ] && printf '%s\n' "$silver" | sed 's/\t/ | /g; s/^/        /'
echo
