-- SCD Type 2 dimension table for sales 
{{
    config(
        materialized='table'
    )
}}


WITH deal_events AS ( 
    SELECT * FROM {{ ref('stg_deal_events') }}
),
-- Create a versioned table of deals with previous values for tracked attributes
deal_versions AS (
    SELECT 
        deal_id,
        deal_owner,
        deal_name,
        deal_value,
        new_stage,
        deal_vertical,
        event_timestamp,
        LAG(deal_owner) OVER (PARTITION BY deal_id ORDER BY event_timestamp) AS prev_owner,
        LAG(deal_name) OVER (PARTITION BY deal_id ORDER BY event_timestamp) AS prev_name,
        LAG(deal_value) OVER (PARTITION BY deal_id ORDER BY event_timestamp) AS prev_value,
        LAG(new_stage) OVER (PARTITION BY deal_id ORDER BY event_timestamp) AS prev_stage,
        LAG(deal_vertical) OVER(PARTITION BY deal_id ORDER BY event_timestamp) AS prev_deal_vertical
    FROM deal_events
),
-- Flag rows where any tracked attribute changed
deal_flags AS (
    SELECT
        deal_id,
        deal_owner,
        deal_name,
        deal_value,
        new_stage,
        deal_vertical,
        event_timestamp,
        CASE 
            WHEN prev_stage IS NULL THEN true
            WHEN new_stage != prev_stage THEN true 
            WHEN deal_value != prev_value THEN true 
            WHEN deal_name != prev_name THEN true 
            WHEN deal_owner != prev_owner THEN true
            WHEN deal_vertical != prev_deal_vertical THEN true
            ELSE false 
        END AS is_new_versions
    FROM deal_versions
),
-- Build version boundaries for each deals
deal_boundaries AS (
    SELECT 
        md5(deal_id || '-' || CAST(event_timestamp AS string) || 
            CAST(ROW_NUMBER() OVER (PARTITION BY deal_id ORDER BY event_timestamp) AS string)
        ) AS deal_surrogate_key,
        deal_id,
        deal_owner,
        deal_name,
        deal_value,
        new_stage AS deal_stage,
        deal_vertical,
        event_timestamp AS valid_from,
        LEAD(event_timestamp) OVER (PARTITION BY deal_id ORDER BY event_timestamp) AS valid_to,
        CASE 
            WHEN LEAD(event_timestamp) OVER (PARTITION BY deal_id ORDER BY event_timestamp) IS NULL THEN true
            ELSE false 
        END AS is_current 
    FROM deal_flags
    WHERE is_new_versions = true 
)
SELECT * FROM deal_boundaries