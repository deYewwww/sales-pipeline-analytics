{{
    config(materialized = 'table')
}}
SELECT 
    -- who 
    deal_owner,
    -- how many 
    COUNT(DISTINCT deal_id) AS total_deals,
    COUNT(DISTINCT CASE 
            WHEN deal_stage IN ('Won', 'Invoiced') THEN deal_id END) AS won_deals,
    -- how good 
    ROUND(COUNT(DISTINCT CASE 
            WHEN deal_stage IN ('Won', 'Invoiced') THEN deal_id END) * 100.0 
            / COUNT(DISTINCT deal_id), 2
    ) AS win_rate,
    -- how much money (won + lost + open = total)
    SUM(deal_value) AS total_value,
    SUM(CASE 
            WHEN deal_stage IN ('Won', 'Invoiced') THEN deal_value
        ELSE 0
    END) AS won_value,
    SUM(CASE 
            WHEN deal_stage = 'Lost' THEN deal_value
        ELSE 0
    END) AS lost_value,
    SUM(CASE
            WHEN deal_stage IN ('Enquiry', 'Quoted') THEN deal_value
        ELSE 0
    END) AS open_value,
    -- average deal value (won deals only)
    COALESCE(ROUND(AVG(CASE 
            WHEN deal_stage IN('Won', 'Invoiced') THEN deal_value 
        END), 2), 
    0) AS avg_deal_value
FROM {{ ref('dim_deals') }}
WHERE is_current = true
GROUP BY 1
ORDER BY won_value DESC