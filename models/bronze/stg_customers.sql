
SELECT
    customer_id,
    name,
    state,
    policy_type
FROM {{ source('insurance', 'customers') }}