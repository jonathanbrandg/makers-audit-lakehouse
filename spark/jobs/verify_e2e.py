"""End-to-end demo + zero-loss check, run once by `docker compose up`.

1. Waits for the .NET core and the Debezium connector.
2. Drives real business traffic through the core API (/api/simulate).
3. Reconciles SOURCE (SQL Server) against BRONZE (Iceberg) by event_id until they
   match or the timeout expires. Exit code 0 = PASS, 1 = FAIL (dbt only runs on PASS).

The source set is every event ever written to the outbox: the rows still in
dbo.AuditOutbox plus every insert captured by CDC (the OLTP retention job may already
have purged them from the outbox, but the CDC change table keeps them).
"""

from __future__ import annotations

import json
import os
import sys
import time
import urllib.request

from pyspark.sql import functions as F

from lake_common import BRONZE_TABLE, build_session, ensure_bronze_table, get_logger

LOG = get_logger("verify_e2e")

CORE_API = os.getenv("CORE_API_URL", "http://core-api:8080")
CONNECT = os.getenv("CONNECT_URL", "http://connect:8083")
CONNECTOR = os.getenv("CONNECTOR_NAME", "audit-outbox-cdc")
ACCOUNTS = int(os.getenv("E2E_ACCOUNTS", "20"))
PAYMENTS = int(os.getenv("E2E_PAYMENTS", "2000"))
TIMEOUT_SECONDS = int(os.getenv("E2E_TIMEOUT_SECONDS", "600"))
POLL_SECONDS = 15
# true = do not call /api/simulate; just reconcile current data (scripts/resultado -Actualizar)
SKIP_TRAFFIC = os.getenv("E2E_SKIP_TRAFFIC", "false").lower() == "true"

JDBC_URL = (
    "jdbc:sqlserver://sqlserver:1433;databaseName=AuditDb;"
    "encrypt=false;trustServerCertificate=true"
)
SOURCE_EVENTS_SQL = """
SELECT LOWER(CONVERT(VARCHAR(36), EventId)) AS event_id FROM dbo.AuditOutbox
UNION
SELECT LOWER(CONVERT(VARCHAR(36), EventId)) FROM cdc.dbo_AuditOutbox_CT WHERE [__$operation] = 2
"""


def http_json(url: str, method: str = "GET") -> dict:
    req = urllib.request.Request(url, method=method, data=b"" if method == "POST" else None)
    with urllib.request.urlopen(req, timeout=300) as resp:
        body = resp.read().decode()
        return json.loads(body) if body.strip().startswith("{") else {"raw": body}


def wait_until(description: str, probe, timeout: int = 300) -> None:
    deadline = time.monotonic() + timeout
    while True:
        try:
            if probe():
                LOG.info("ready: %s", description)
                return
        except Exception:  # noqa: BLE001 - service still starting
            pass
        if time.monotonic() > deadline:
            raise TimeoutError(f"timeout waiting for {description}")
        time.sleep(5)


def connector_running() -> bool:
    status = http_json(f"{CONNECT}/connectors/{CONNECTOR}/status")
    tasks = status.get("tasks", [])
    return status["connector"]["state"] == "RUNNING" and bool(tasks) and all(
        t["state"] == "RUNNING" for t in tasks
    )


def main() -> int:
    wait_until("core-api /health", lambda: http_json(f"{CORE_API}/health").get("raw") == "Healthy")
    wait_until("debezium connector RUNNING", connector_running)

    if SKIP_TRAFFIC:
        LOG.info("E2E_SKIP_TRAFFIC=true: only reconciling what is already in SQL Server")
        sim: dict = {}
    else:
        LOG.info("generating traffic: %s accounts, %s payments via core API", ACCOUNTS, PAYMENTS)
        sim = http_json(
            f"{CORE_API}/api/simulate?accounts={ACCOUNTS}&payments={PAYMENTS}", method="POST"
        )
        LOG.info("core API result: %s", sim)
    traffic_done = time.monotonic()

    spark = build_session("audit-e2e-verify")
    ensure_bronze_table(spark)
    password = os.environ["MSSQL_SA_PASSWORD"]

    deadline = traffic_done + TIMEOUT_SECONDS
    while True:
        source = (
            spark.read.format("jdbc")
            .option("url", JDBC_URL)
            .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
            .option("query", SOURCE_EVENTS_SQL)
            .option("user", "sa")
            .option("password", password)
            .load()
            .cache()
        )
        spark.sql(f"REFRESH TABLE {BRONZE_TABLE}")
        bronze = spark.table(BRONZE_TABLE)

        stats = bronze.agg(
            F.count("*").alias("rows"), F.countDistinct("event_id").alias("distinct")
        ).first()
        source_count = source.count()
        missing = source.join(bronze.select("event_id"), "event_id", "left_anti").count()
        duplicates = stats["rows"] - stats["distinct"]
        source.unpersist()

        LOG.info(
            "source=%s bronze_rows=%s bronze_distinct=%s missing=%s duplicates=%s",
            source_count, stats["rows"], stats["distinct"], missing, duplicates,
        )
        passed = missing == 0 and duplicates == 0
        if passed or time.monotonic() > deadline:
            report(passed, sim, source_count, stats["distinct"], missing, duplicates,
                   time.monotonic() - traffic_done)
            return 0 if passed else 1
        time.sleep(POLL_SECONDS)


def report(passed: bool, sim: dict, source: int, lake: int, missing: int,
           duplicates: int, seconds: float) -> None:
    """Plain-language summary. Lines start with '>> ' so scripts/resultado.* can extract them."""
    lines = [
        "=" * 64,
        "PRUEBA END-TO-END: " + ("APROBADA (E2E PASS)" if passed else "FALLIDA (E2E FAIL)"),
        "SQL Server -> Debezium -> Kafka -> Spark -> Iceberg",
        "",
        (f"Operaciones de negocio simuladas : {sim.get('accounts', 0)} cuentas, "
         f"{sim.get('payments', 0)} pagos, {sim.get('reversals', 0)} reversos")
        if sim else "Operaciones de negocio simuladas : ninguna (solo verificacion de lo existente)",
        f"Eventos en SQL Server (acumulado) : {source}",
        f"Eventos en el lakehouse (acumul.) : {lake}",
        f"Eventos perdidos                  : {missing}",
        f"Eventos duplicados                : {duplicates}",
        f"Tiempo hasta quedar sincronizado  : {seconds:.0f} s",
        "=" * 64,
    ]
    print("\n" + "\n".join(">> " + line for line in lines) + "\n", flush=True)


if __name__ == "__main__":
    sys.exit(main())
