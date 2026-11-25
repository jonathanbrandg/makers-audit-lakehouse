{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['activity_date', 'actor']
  )
}}

{#- Gold: who did what per day (compliance reviews, anomaly detection on actors). -#}
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
    event_date                                          as activity_date,
    actor,
    count(*)                                            as events,
    sum(case when action = 'DELETE' then 1 else 0 end)  as delete_events,
    count(distinct entity_type)                         as distinct_entity_types,
    count(distinct entity_id)                           as distinct_entities,
    coalesce(sum(amount), 0)                            as total_amount,
    current_timestamp()                                 as updated_at
from scoped
group by event_date, actor
