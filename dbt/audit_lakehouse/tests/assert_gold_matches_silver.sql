-- Gold totals must reconcile with silver per day (catches broken incremental logic).
with silver as (

    select event_date as activity_date, count(*) as events
    from {{ ref('audit_events') }}
    group by event_date

),

gold as (

    select activity_date, sum(events) as events
    from {{ ref('fct_audit_daily_activity') }}
    group by activity_date

)

select
    coalesce(s.activity_date, g.activity_date) as activity_date,
    s.events as silver_events,
    g.events as gold_events
from silver s
full outer join gold g on s.activity_date = g.activity_date
where coalesce(s.events, -1) <> coalesce(g.events, -1)
