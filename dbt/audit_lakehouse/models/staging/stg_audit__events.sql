{#-
  Staging: typing and contract normalization only (no business rules).
  The payload contract is versioned: each schema_version gets its own parser,
  so a new version in .NET is an additive change here, not a bronze rewrite.
-#}
with source as (

    select * from {{ source('audit_bronze', 'events_bronze') }}

),

parsed as (

    select
        event_id,
        event_ts,
        to_date(event_ts)               as event_date,
        upper(trim(entity_type))        as entity_type,
        entity_id,
        upper(trim(action))             as action,
        lower(trim(actor))              as actor,
        schema_version,
        case schema_version
            when 1 then from_json(payload, 'amount DECIMAL(18,2), channel STRING')
        end                             as payload_v,
        payload                         as payload_raw,
        ingested_at                     as bronze_ingested_at
    from source

)

select
    event_id,
    event_ts,
    event_date,
    entity_type,
    entity_id,
    action,
    actor,
    schema_version,
    payload_v.amount                    as amount,
    payload_v.channel                   as channel,
    payload_raw,
    bronze_ingested_at
from parsed
