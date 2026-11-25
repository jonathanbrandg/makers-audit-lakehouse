{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='event_id',
    partition_by=['days(event_ts)'],
    on_schema_change='append_new_columns',
    tblproperties={
      'format-version': '2',
      'write.target-file-size-bytes': '134217728',
      'write.merge.mode': 'merge-on-read',
      'write.update.mode': 'merge-on-read',
      'write.delete.mode': 'merge-on-read'
    }
  )
}}

{#-
  Silver: conformed audit trail.
  Incremental by bronze ingestion time with a lookback window. Rows of the window
  that are already in silver are removed with an anti-join BEFORE the MERGE, so the
  MERGE only inserts: an append-only audit table never rewrites existing files.
  (Measured: without the anti-join, copy-on-write MERGE rewrote the whole day
  partition, 19,500 rows, to add 2,500.) The MERGE on event_id remains as the
  idempotency guard.
-#}
with source as (

    select * from {{ ref('stg_audit__events') }}

    {% if is_incremental() %}
    where bronze_ingested_at > (
        select coalesce(max(bronze_ingested_at), timestamp '1970-01-01')
               - interval {{ var('silver_lookback_minutes') }} minutes
        from {{ this }}
    )
    {% endif %}

)

{% if is_incremental() %}
, already_loaded as (

    select event_id
    from {{ this }}
    where bronze_ingested_at > (
        select coalesce(max(bronze_ingested_at), timestamp '1970-01-01')
               - interval {{ var('silver_lookback_minutes') }} minutes
        from {{ this }}
    )

)
{% endif %}

select
    s.event_id,
    s.event_ts,
    s.event_date,
    s.entity_type,
    s.entity_id,
    s.action,
    s.actor,
    s.schema_version,
    s.amount,
    s.channel,
    s.payload_raw,
    s.bronze_ingested_at,
    current_timestamp()             as dbt_loaded_at
from source s
{% if is_incremental() %}
left anti join already_loaded a
    on a.event_id = s.event_id
{% endif %}
