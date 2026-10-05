-- Returns reps whose value buckets dont add up to their total.

SELECT
    deal_owner,
    total_value,
    won_value + lost_value + open_value AS parts_value
FROM {{ ref('gold_rep_performance') }}
WHERE won_value + lost_value + open_value != total_value 