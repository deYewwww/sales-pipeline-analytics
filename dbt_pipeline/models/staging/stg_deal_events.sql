WITH source AS (
    SELECT * FROM {{ source('silver', 'deal_events_cleaned') }}
)
SELECT
    event_id,
    deal_id,
    event_type,
    owner AS deal_owner,
    deal_name,
    deal_value_rm AS deal_value,
    old_stage,
    new_stage,
    event_timestamp,
    metadata_source,
    metadata_vertical AS deal_vertical,
    _kafka_partition,
    _kafka_offset,
    _kafka_timestamp,
    _ingested_at,
    _quality_flags
FROM source