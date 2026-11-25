"""Iceberg table maintenance for the audit lake (run on a schedule, e.g. daily).

1. Retention:  delete whole day partitions older than AUDIT_RETENTION_DAYS
               (aligned to day boundaries => metadata-only delete, no rewrite).
2. Compaction: rewrite_data_files (sort by the table write order) to fix the
               small-files problem created by streaming micro-batches.
3. Manifests:  rewrite_manifests to keep scan planning fast.
4. Snapshots:  expire_snapshots (bounded time travel + frees storage).
5. Orphans:    remove_orphan_files, keeping the 3-day safety default so files of
               in-flight commits are never deleted.

Never use S3 lifecycle expiration on table prefixes: it deletes files still
referenced by Iceberg metadata and corrupts the table.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

from pyspark.sql import SparkSession

from lake_common import BRONZE_TABLE, build_session, ensure_bronze_table, get_logger

LOG = get_logger("maintenance")

TABLE_IDENT = BRONZE_TABLE.split(".", 1)[1]  # procedures take db.table (py3.8 in image)
INCLUDE_OPEN = os.getenv("COMPACT_INCLUDE_OPEN_PARTITIONS", "false").lower() == "true"
RETENTION_DAYS = int(os.getenv("AUDIT_RETENTION_DAYS", "2555"))
SNAPSHOT_RETENTION_HOURS = int(os.getenv("SNAPSHOT_RETENTION_HOURS", "24"))
SNAPSHOT_RETAIN_LAST = int(os.getenv("SNAPSHOT_RETAIN_LAST", "10"))


def file_stats(spark: SparkSession, label: str) -> None:
    row = spark.sql(
        f"""
        SELECT COUNT(*)                                   AS data_files,
               COALESCE(SUM(record_count), 0)             AS records,
               ROUND(AVG(file_size_in_bytes) / 1048576, 3) AS avg_file_mb
        FROM {BRONZE_TABLE}.files
        WHERE content = 0
        """
    ).first()
    snapshots = spark.sql(f"SELECT COUNT(*) AS n FROM {BRONZE_TABLE}.snapshots").first()
    LOG.info(
        "[%s] data_files=%s records=%s avg_file_mb=%s snapshots=%s",
        label,
        row["data_files"],
        row["records"],
        row["avg_file_mb"],
        snapshots["n"],
    )


def main() -> None:
    spark = build_session("audit-lake-maintenance")
    ensure_bronze_table(spark)
    now = datetime.now(timezone.utc)
    today = now.replace(hour=0, minute=0, second=0, microsecond=0)

    file_stats(spark, "before")

    # 1. Retention (day-aligned => Iceberg drops whole partitions from metadata)
    cutoff = today - timedelta(days=RETENTION_DAYS)
    spark.sql(
        f"DELETE FROM {BRONZE_TABLE} "
        f"WHERE event_ts < TIMESTAMP '{cutoff:%Y-%m-%d %H:%M:%S}'"
    )
    LOG.info("retention: deleted events before %s", cutoff.date())

    # 2. Compaction. In production only closed partitions (< today) are rewritten,
    #    so compaction never competes with the streaming writer for the same files.
    where = "" if INCLUDE_OPEN else f", where => \"event_ts < TIMESTAMP '{today:%Y-%m-%d %H:%M:%S}'\""
    result = spark.sql(
        f"""
        CALL lake.system.rewrite_data_files(
            table    => '{TABLE_IDENT}',
            strategy => 'sort',
            options  => map(
                'min-input-files', '2',
                'target-file-size-bytes', '134217728',
                'partial-progress.enabled', 'true')
            {where})
        """
    ).first()
    LOG.info("compaction: %s", result.asDict() if result else None)

    # 3. Manifests
    spark.sql(f"CALL lake.system.rewrite_manifests('{TABLE_IDENT}')")

    # 4. Snapshot expiration
    expire_before = now - timedelta(hours=SNAPSHOT_RETENTION_HOURS)
    result = spark.sql(
        f"""
        CALL lake.system.expire_snapshots(
            table       => '{TABLE_IDENT}',
            older_than  => TIMESTAMP '{expire_before:%Y-%m-%d %H:%M:%S}',
            retain_last => {SNAPSHOT_RETAIN_LAST})
        """
    ).first()
    LOG.info("expire_snapshots: %s", result.asDict() if result else None)

    # 5. Orphan files (default older_than = 3 days; do not lower it).
    #    prefix_listing uses S3FileIO instead of a Hadoop FileSystem for s3://.
    orphans = spark.sql(
        f"CALL lake.system.remove_orphan_files("
        f"table => '{TABLE_IDENT}', prefix_listing => true)"
    ).count()
    LOG.info("remove_orphan_files: removed=%s", orphans)

    file_stats(spark, "after")
    spark.stop()


if __name__ == "__main__":
    main()
