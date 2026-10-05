{{
    config(materialized = 'table')
}}
WITH current_deals AS (
    SELECT 
        deal_id,
        deal_stage,
        deal_value,
        CASE deal_stage
            WHEN 'Enquiry'  THEN 0.1
            WHEN 'Quoted'   THEN 0.3 
            WHEN 'Won'      THEN 0.7
            WHEN 'Invoiced' Then 1.0 
            ELSE 0 
        END AS win_probability      
    FROM {{ ref('dim_deals') }}
    WHERE is_current = true         -- latest version of each deal only 
        AND deal_stage != 'Lost'    -- exclude lost deals from forecast 
)   
SELECT 
    deal_stage,
    COUNT(DISTINCT deal_id) AS deal_count,
    SUM(deal_value) AS total_value,
    win_probability,
    -- multiply per deal then sum to get weighted value per stage
    ROUND(SUM(deal_value * win_probability), 2) AS weighted_value   
FROM current_deals
GROUP BY deal_stage, win_probability    
ORDER BY 
    CASE deal_stage 
        WHEN 'Enquiry' THEN 1
        WHEN 'Quoted' THEN 2
        WHEN 'Won' THEN 3
        WHEN 'Invoiced' THEN 4
    END