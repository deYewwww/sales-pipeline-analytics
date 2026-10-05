-- Return lost deals that have NO matching lost transition.

SELECT 
    d.deal_id
FROM {{ ref('dim_deals') }} AS d
LEFT JOIN {{ ref('fct_stage_transitions') }} AS t
    ON d.deal_id = t.deal_id
    AND t.to_stage = 'Lost'
WHERE d.is_current = true
    AND d.deal_stage = 'Lost'
    AND t.deal_id IS NULL 