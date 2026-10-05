-- Return stages where the Next stage has more deals than this 

SELECT 
    stage,
    deal_entered,
    next_stage_entered
FROM {{ ref('gold_conversion_funnel')}}
WHERE next_stage_entered > deal_entered