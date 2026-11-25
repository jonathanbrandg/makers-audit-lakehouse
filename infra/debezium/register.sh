#!/bin/sh
# Registra (idempotente: PUT .../config) el conector Debezium de la outbox.
set -eu

CONNECT_URL="http://connect:8083"
NAME="audit-outbox-cdc"

sed "s|__MSSQL_SA_PASSWORD__|${MSSQL_SA_PASSWORD}|" /debezium/sqlserver-audit-connector.json > /tmp/connector.json

# La respuesta repite la config (incluye el password): no la imprimimos.
curl -fsS -o /dev/null -w "PUT config -> HTTP %{http_code}\n" -X PUT \
  -H "Content-Type: application/json" \
  --data @/tmp/connector.json \
  "${CONNECT_URL}/connectors/${NAME}/config"
rm -f /tmp/connector.json

sleep 5
curl -fsS "${CONNECT_URL}/connectors/${NAME}/status"
echo
