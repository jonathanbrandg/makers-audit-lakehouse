#!/bin/bash
# Simula el core .NET escribiendo eventos de auditoría en la outbox.
# LOAD_BATCHES=0 => sin fin (Ctrl+C / docker compose stop load-generator).
set -euo pipefail

ROWS="${LOAD_BATCH_ROWS:-500}"
INTERVAL="${LOAD_INTERVAL_SECONDS:-2}"
BATCHES="${LOAD_BATCHES:-0}"
SQLCMD=/opt/mssql-tools18/bin/sqlcmd

i=0
while [ "$BATCHES" -eq 0 ] || [ "$i" -lt "$BATCHES" ]; do
  i=$((i + 1))
  "$SQLCMD" -S sqlserver -U sa -P "$MSSQL_SA_PASSWORD" -C -b -d AuditDb -Q "
    SET NOCOUNT ON;
    INSERT INTO dbo.AuditOutbox (EntityType, EntityId, Action, Actor, Payload)
    SELECT TOP (${ROWS})
        CASE ABS(CHECKSUM(NEWID())) % 3 WHEN 0 THEN N'Account' WHEN 1 THEN N'Payment' ELSE N'Customer' END,
        CONCAT(N'E-', ABS(CHECKSUM(NEWID())) % 5000),
        CASE ABS(CHECKSUM(NEWID())) % 3 WHEN 0 THEN N'CREATE' WHEN 1 THEN N'UPDATE' ELSE N'DELETE' END,
        CONCAT(N'user-', ABS(CHECKSUM(NEWID())) % 50),
        CONCAT(N'{\"amount\":', ABS(CHECKSUM(NEWID())) % 100000, N',\"channel\":\"load\",\"batch\":${i}}')
    FROM sys.all_objects a CROSS JOIN sys.all_objects b;"
  echo "$(date -u +%FT%TZ) batch=${i} rows=${ROWS}"
  sleep "$INTERVAL"
done
