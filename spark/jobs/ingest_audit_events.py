"""Bronze ingestion: Debezium CDC (Kafka) -> Iceberg `lake.audit.events_bronze`.

Delivery guarantees
-------------------
* Kafka offsets are tracked in the Structured Streaming checkpoint. If this job dies
  mid-peak, the next run resumes from the last committed batch: no event is skipped
  while it is still inside the topic retention window (7 days in the PoC).
* `failOnDataLoss=true`: if offsets expired before being read we fail loudly instead
  of silently skipping data.
* foreachBatch is at-least-once, so each batch is written with an insert-only MERGE
  keyed by `event_id`. Re-processing a batch (or a Debezium re-snapshot) is a no-op.
* `maxOffsetsPerTrigger` bounds each micro-batch, so a peak in .NET turns into lag
  on the topic instead of memory pressure here.
"""

from __future__ import annotations

import os

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from lake_common import (
    BRONZE_TABLE,
    DEBEZIUM_ENVELOPE_SCHEMA,
    build_session,
    ensure_bronze_table,
    get_logger,
)

LOG = get_logger("ingest_audit_events")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "redpanda:29092")
AUDIT_TOPIC = os.getenv("AUDIT_TOPIC", "mssql.AuditDb.dbo.AuditOutbox")
MAX_OFFSETS_PER_TRIGGER = os.getenv("MAX_OFFSETS_PER_TRIGGER", "50000")
TRIGGER_MODE = os.getenv("TRIGGER_MODE", "processing")  # processing | available_now
TRIGGER_INTERVAL = os.getenv("TRIGGER_INTERVAL", "30 seconds")
CHECKPOINT = os.getenv(
    "CHECKPOINT_LOCATION", "/opt/spark/checkpoints/audit_events_bronze"
)

BRONZE_COLUMNS = [
    "event_id",
    "event_ts",
    "entity_type",
    "entity_id",
    "action",
    "actor",
    "schema_version",
    "payload",
    "cdc_op",
    "cdc_commit_lsn",
    "kafka_partition",
    "kafka_offset",
    "ingested_at",
]


def parse_cdc(raw: DataFrame) -> DataFrame:
    """Kafka records -> flat bronze rows. Built-in functions only (no Python UDFs)."""
    envelope = F.from_json(F.col("value").cast("string"), DEBEZIUM_ENVELOPE_SCHEMA)
    return (
        raw.where(F.col("value").isNotNull())  # tombstones
        .select(
            envelope.alias("e"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
        )
        # Audit is append-only: keep snapshot (r) and inserts (c). Outbox cleanup
        # deletes (d) in SQL Server must NOT delete history in the lake.
        .where(F.col("e.op").isin("r", "c") & F.col("e.after").isNotNull())
        .select(
            F.lower(F.col("e.after.EventId")).alias("event_id"),
            F.timestamp_millis(F.col("e.after.OccurredAt")).alias("event_ts"),
            F.col("e.after.EntityType").alias("entity_type"),
            F.col("e.after.EntityId").alias("entity_id"),
            F.col("e.after.Action").alias("action"),
            F.col("e.after.Actor").alias("actor"),
            F.col("e.after.SchemaVersion").alias("schema_version"),
            F.col("e.after.Payload").alias("payload"),
            F.col("e.op").alias("cdc_op"),
            F.col("e.source.commit_lsn").alias("cdc_commit_lsn"),
            "kafka_partition",
            "kafka_offset",
            F.current_timestamp().alias("ingested_at"),
        )
    )


def write_batch(batch: DataFrame, batch_id: int) -> None:
    """Idempotent insert-only MERGE, pruned to the batch's day partitions."""
    deduped = batch.dropDuplicates(["event_id"]).select(*BRONZE_COLUMNS)
    # Reused twice (stats + MERGE): persist, then release.
    deduped.persist()
    try:
        stats = deduped.agg(
            F.count(F.lit(1)).alias("rows"),
            F.min("event_ts").alias("min_ts"),
            F.max("event_ts").alias("max_ts"),
        ).first()
        if stats is None or stats["rows"] == 0:
            LOG.info("batch=%s empty", batch_id)
            return

        view = f"bronze_src_{batch_id}"
        deduped.createOrReplaceTempView(view)
        # The event_ts range on the target lets Iceberg prune to the day partitions
        # touched by this batch instead of scanning the whole table.
        deduped.sparkSession.sql(
            f"""
            MERGE INTO {BRONZE_TABLE} t
            USING {view} s
            ON  t.event_id = s.event_id
            AND t.event_ts BETWEEN TIMESTAMP '{stats["min_ts"]}' AND TIMESTAMP '{stats["max_ts"]}'
            WHEN NOT MATCHED THEN INSERT *
            """
        )
        LOG.info(
            "batch=%s rows=%s event_ts=[%s, %s] partitions=%s",
            batch_id,
            stats["rows"],
            stats["min_ts"],
            stats["max_ts"],
            deduped.rdd.getNumPartitions(),
        )
    finally:
        deduped.unpersist()


def main() -> None:
    spark = build_session("audit-bronze-ingest")
    ensure_bronze_table(spark)

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", AUDIT_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "true")
        .option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)
        .load()
    )

    writer = (
        parse_cdc(raw)
        .writeStream.queryName("audit_events_bronze")
        .foreachBatch(write_batch)
        .option("checkpointLocation", CHECKPOINT)
    )
    writer = (
        writer.trigger(availableNow=True)
        if TRIGGER_MODE == "available_now"
        else writer.trigger(processingTime=TRIGGER_INTERVAL)
    )

    LOG.info(
        "starting topic=%s trigger=%s/%s checkpoint=%s",
        AUDIT_TOPIC,
        TRIGGER_MODE,
        TRIGGER_INTERVAL,
        CHECKPOINT,
    )
    writer.start().awaitTermination()


if __name__ == "__main__":
    main()
