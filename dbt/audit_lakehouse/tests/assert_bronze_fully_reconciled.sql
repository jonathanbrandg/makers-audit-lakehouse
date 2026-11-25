-- Zero-loss control between layers: every bronze event already covered by the
-- last silver load must exist in silver. Any returned row is a lost event.
with silver_watermark as (

    select max(bronze_ingested_at) as max_ingested_at
    from {{ ref('audit_events') }}

)

select b.event_id, b.event_ts, b.ingested_at
from {{ source('audit_bronze', 'events_bronze') }} b
cross join silver_watermark w
left join {{ ref('audit_events') }} s
    on s.event_id = b.event_id
where b.ingested_at <= w.max_ingested_at
  and s.event_id is null
