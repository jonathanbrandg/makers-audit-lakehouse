"""Evidence for the README: counts, duplicates, file layout and snapshot history."""

from __future__ import annotations

from lake_common import BRONZE_TABLE, build_session, ensure_bronze_table


def main() -> None:
    spark = build_session("audit-lake-inspect")
    ensure_bronze_table(spark)

    print("\n== Volumen y duplicados (debe ser duplicates = 0) ==")
    spark.sql(
        f"""
        SELECT COUNT(*)                            AS rows,
               COUNT(DISTINCT event_id)            AS distinct_events,
               COUNT(*) - COUNT(DISTINCT event_id) AS duplicates,
               MIN(event_ts) AS min_event_ts,
               MAX(event_ts) AS max_event_ts
        FROM {BRONZE_TABLE}
        """
    ).show(truncate=False)

    print("\n== Archivos por partición (small files) ==")
    spark.sql(
        f"""
        SELECT partition,
               COUNT(*)                                    AS data_files,
               SUM(record_count)                           AS records,
               ROUND(AVG(file_size_in_bytes) / 1048576, 3) AS avg_file_mb
        FROM {BRONZE_TABLE}.files
        WHERE content = 0
        GROUP BY partition
        ORDER BY partition
        """
    ).show(truncate=False)

    print("\n== Snapshots (ACID / time travel) ==")
    spark.sql(
        f"""
        SELECT committed_at, snapshot_id, operation,
               summary['added-records']    AS added_records,
               summary['added-data-files'] AS added_files
        FROM {BRONZE_TABLE}.snapshots
        ORDER BY committed_at DESC
        LIMIT 20
        """
    ).show(truncate=False)

    print("\n== Lag de ingesta (ingested_at - event_ts) ==")
    spark.sql(
        f"""
        SELECT ROUND(AVG(unix_timestamp(ingested_at) - unix_timestamp(event_ts)), 1) AS avg_lag_s,
               MAX(unix_timestamp(ingested_at) - unix_timestamp(event_ts))           AS max_lag_s
        FROM {BRONZE_TABLE}
        """
    ).show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
