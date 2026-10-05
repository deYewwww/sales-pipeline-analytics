{{
    config(
        materialized='table'
    )
}}

-- generate one row per day from earliest deal to today
WITH date_spine AS (
    SELECT 
        EXPLODE(SEQUENCE (
            (SELECT CAST(MIN(valid_from) AS DATE) FROM {{ ref('dim_deals') }}), 
            CURRENT_DATE(),
            INTERVAL 1 DAY
        )) AS snapshot_date
),
deal_versions AS (
    SELECT 
        *
    FROM {{ ref('dim_deals') }}
),
snapshot AS (
    SELECT 
        ds.snapshot_date,
        dd.deal_id,
        dd.deal_owner,
        dd.deal_name,
        dd.deal_value,
        dd.deal_stage,
        dd.deal_vertical
    FROM date_spine AS ds
    CROSS JOIN deal_versions AS dd
    WHERE ds.snapshot_date >= CAST(dd.valid_from AS DATE) 
        AND (ds.snapshot_date < CAST(dd.valid_to AS DATE) OR dd.valid_to IS NULL)
)
SELECT 
    snapshot_date,
    deal_id,
    deal_owner,
    deal_name,
    deal_value,
    deal_stage,
    deal_vertical 
FROM snapshot