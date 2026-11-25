"""Shared definitions for the audit lakehouse jobs: session, schemas and table DDL."""

from __future__ import annotations

import logging
import os

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

NAMESPACE = "lake.audit"
BRONZE_TABLE = f"{NAMESPACE}.events_bronze"

# Debezium "after" image of dbo.AuditOutbox (JsonConverter, schemas disabled).
# DATETIME2(3) arrives as epoch millis (time.precision.mode=adaptive).
OUTBOX_ROW_SCHEMA = StructType(
    [
        StructField("EventId", StringType(), False),
        StructField("OccurredAt", LongType(), False),
        StructField("EntityType", StringType(), False),
        StructField("EntityId", StringType(), False),
        StructField("Action", StringType(), False),
        StructField("Actor", StringType(), False),
        StructField("SchemaVersion", IntegerType(), False),
        StructField("Payload", StringType(), True),
    ]
)

DEBEZIUM_ENVELOPE_SCHEMA = StructType(
    [
        StructField("before", OUTBOX_ROW_SCHEMA, True),
        StructField("after", OUTBOX_ROW_SCHEMA, True),
        StructField(
            "source",
            StructType(
                [
                    StructField("change_lsn", StringType(), True),
                    StructField("commit_lsn", StringType(), True),
                ]
            ),
            True,
        ),
        StructField("op", StringType(), False),
        StructField("ts_ms", LongType(), True),
    ]
)

BRONZE_DDL = f"""
CREATE TABLE IF NOT EXISTS {BRONZE_TABLE} (
    event_id        STRING    NOT NULL COMMENT 'Outbox EventId (idempotency key)',
    event_ts        TIMESTAMP NOT NULL COMMENT 'Business time (UTC) when the event happened',
    entity_type     STRING    NOT NULL,
    entity_id       STRING    NOT NULL,
    action          STRING    NOT NULL,
    actor           STRING    NOT NULL,
    schema_version  INT       NOT NULL COMMENT 'Version of the payload contract',
    payload         STRING             COMMENT 'Raw JSON payload, parsed downstream by dbt',
    cdc_op          STRING    NOT NULL COMMENT 'Debezium op: r=snapshot, c=insert',
    cdc_commit_lsn  STRING             COMMENT 'SQL Server commit LSN (lineage / reconciliation)',
    kafka_partition INT       NOT NULL,
    kafka_offset    BIGINT    NOT NULL,
    ingested_at     TIMESTAMP NOT NULL
)
USING iceberg
PARTITIONED BY (days(event_ts))
TBLPROPERTIES (
    'format-version'                            = '2',
    'write.target-file-size-bytes'              = '134217728',
    'write.delete.mode'                         = 'merge-on-read',
    'write.update.mode'                         = 'merge-on-read',
    'write.merge.mode'                          = 'merge-on-read',
    'write.metadata.delete-after-commit.enabled'= 'true',
    'write.metadata.previous-versions-max'      = '50',
    'commit.retry.num-retries'                  = '10',
    'history.expire.max-snapshot-age-ms'        = '86400000'
)
"""

# Hash distribution by partition + local sort: one writer per day partition per task
# (fewer files) and row groups ordered for entity lookups.
BRONZE_WRITE_ORDER = (
    f"ALTER TABLE {BRONZE_TABLE} "
    "WRITE DISTRIBUTED BY PARTITION LOCALLY ORDERED BY entity_id, event_ts"
)


def get_logger(name: str) -> logging.Logger:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    return logging.getLogger(name)


def build_session(app_name: str) -> SparkSession:
    """Session config lives in spark-defaults.conf so the same jobs run on Glue/EMR."""
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def ensure_bronze_table(spark: SparkSession) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {NAMESPACE}")
    spark.sql(BRONZE_DDL)
    spark.sql(BRONZE_WRITE_ORDER)
