{{ 
    config(materialized = 'table')
}}

WITH stage_stats AS(
    SELECT 
        from_stage AS stage,
        AVG(hours_in_stage) / 24 AS raw_avg_days,
        MEDIAN(hours_in_stage) / 24 AS raw_median_days,
        MIN(hours_in_stage) / 24 AS raw_min_days,
        MAX(hours_in_stage) / 24 As raw_max_days,
        COUNT(DISTINCT deal_id) AS deal_count
    FROM {{ ref('fct_stage_transitions') }}
    GROUP BY 1
)
SELECT 
    stage,
    deal_count,
    ROUND(raw_avg_days, 2) AS avg_days,
    ROUND(raw_median_days, 2) AS median_days,
    ROUND(raw_min_days, 2) AS min_days,
    ROUND(raw_max_days, 2) AS max_days,
    CASE
        WHEN raw_median_days = MAX(raw_median_days) OVER () THEN true 
        ELSE false 
    END AS is_bottleneck
FROM stage_stats
ORDER BY    
    CASE stage
        WHEN 'Enquiry'  THEN 1 
        WHEN 'Quoted'   THEN 2 
        WHEN 'Won'      THEN 3 
        WHEN 'Invoiced' THEN 4
    END