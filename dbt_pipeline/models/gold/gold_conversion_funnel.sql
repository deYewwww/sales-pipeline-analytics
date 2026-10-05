{{
    config(materialized='table')
}}

WITH stage_reached AS (
    -- Every deal starts at Enquiry 
    SELECT 
        'Enquiry' AS stage,
        1 AS stage_order,
        COUNT(DISTINCT deal_id) AS deal_entered
    FROM {{ ref('dim_deals') }}
    WHERE is_current = true

    UNION ALL 

    -- Later stages: count deals that ever arrived there
    SELECT 
        to_stage AS stage,
        CASE to_stage
            WHEN 'Quoted'   THEN 2 
            WHEN 'Won'      THEN 3
            WHEN 'Invoiced' THEN 4
        END AS stage_order,
        COUNT(DISTINCT deal_id) AS deal_entered
    FROM {{ ref('fct_stage_transitions') }}
    WHERE to_stage IN ('Quoted', 'Won', 'Invoiced')
    GROUP BY stage, stage_order
),
with_next AS (
    SELECT 
        stage,
        stage_order,
        deal_entered,
        LEAD(deal_entered) OVER(ORDER BY stage_order) AS next_stage_entered
    FROM stage_reached
)
SELECT 
    stage,
    deal_entered,
    next_stage_entered,
    ROUND(next_stage_entered * 100.0 / deal_entered, 2) AS conversion_rate
FROM with_next
ORDER BY stage_order
