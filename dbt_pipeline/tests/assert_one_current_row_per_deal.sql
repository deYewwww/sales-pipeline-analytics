-- Return deals that do not have exactly one current row 

SELECT 
    deal_id,
    COUNT(CASE WHEN is_current = true THEN 1 END) AS current_row
FROM {{ ref('dim_deals') }}
GROUP BY deal_id
HAVING COUNT(CASE WHEN is_current = true THEN 1 END) != 1