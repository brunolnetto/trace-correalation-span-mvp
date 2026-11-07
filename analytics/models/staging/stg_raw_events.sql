{{ config(materialized='view') }}

{% set raw_events_path = env_var('RAW_EVENTS_PATH', var('raw_events_path')) %}

with raw_records as (
    select json as record_json
    from read_ndjson_objects('{{ raw_events_path }}', ignore_errors = true)
),

extracted as (
    select
        json_extract_string(record_json, '$.event_id') as event_id,
        try_cast(
            json_extract_string(record_json, '$.ingested_at')
            as timestamptz
        ) as ingested_at,
        json_extract_string(record_json, '$.event.schema_version')
            as schema_version,
        try_cast(
            json_extract_string(record_json, '$.event.occurred_at')
            as timestamptz
        ) as occurred_at,
        json_extract_string(record_json, '$.event.service') as service,
        json_extract_string(record_json, '$.event.logger') as logger,
        json_extract_string(record_json, '$.event.level') as level,
        json_extract_string(record_json, '$.event.event_name') as event_name,
        json_extract_string(record_json, '$.event.correlation_id')
            as correlation_id,
        json_extract_string(record_json, '$.event.trace_id') as trace_id,
        json_extract_string(record_json, '$.event.span_id') as span_id,
        json_extract(record_json, '$.event.attributes') as attributes,
        json_extract(record_json, '$.event.source') as source,
        record_json as raw_record
    from raw_records
)

select *
from extracted
