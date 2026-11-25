{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['activity_date', 'entity_type', 'action']
  )
}}

{#-
  Gold: daily operational / regulatory view. On incremental runs only the days
  touched by newly loaded silver rows are recomputed (late events included),
  then merged on the grain. Small table => no partitioning (avoids tiny files).
-#}
with silver as (

    select * from {{ ref('audit_events') }}

),

{% if is_incremental() %}
affected_days as (

    select distinct event_date
    from silver
    where dbt_loaded_at > (select coalesce(max(updated_at), timestamp '1970-01-01') from {{ this }})

),
{% endif %}

scoped as (

    select s.*
    from silver s
    {% if is_incremental() %}
    join affected_days d on s.event_date = d.event_date
    {% endif %}

)

select
    event_date                      as activity_date,
    entity_type,
    action,
    count(*)                        as events,
    count(distinct entity_id)       as distinct_entities,
    count(distinct actor)           as distinct_actors,
    coalesce(sum(amount), 0)        as total_amount,
    min(event_ts)                   as first_event_ts,
    max(event_ts)                   as last_event_ts,
    current_timestamp()             as updated_at
from scoped
group by event_date, entity_type, action
