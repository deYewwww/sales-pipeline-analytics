{{
    config(
        materialized='table'
    )
}}

WITH stage_events AS(
    SELECT 
        deal_id,
        event_type,
        deal_owner,
        deal_name,
        deal_value,
        deal_vertical,
        old_stage,
        new_stage,
        event_timestamp,
        LAG(event_timestamp) OVER(PARTITION BY deal_id ORDER BY event_timestamp) AS prev_transitioned_at
    FROM {{ ref('stg_deal_events') }}
    WHERE event_type IN ('stage_changed', 'deal_created', 'deal_lost')
),
transitions AS (
    SELECT 
        deal_id,
        event_type,
        deal_owner,
        deal_name,
        deal_value,
        deal_vertical,
        old_stage AS from_stage,
        new_stage AS to_stage,
        event_timestamp AS transitioned_at,
        TIMESTAMPDIFF(HOUR, prev_transitioned_at, event_timestamp) AS hours_in_stage
    FROM stage_events
)
SELECT 
    deal_id,
    deal_owner,
    deal_name,
    deal_value,
    deal_vertical,
    from_stage,
    to_stage,
    transitioned_at,
    hours_in_stage
FROM transitions
WHERE event_type IN ('stage_changed', 'deal_lost')