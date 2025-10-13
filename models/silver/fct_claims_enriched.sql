WITH claims AS (
    SELECT * FROM {{ ref('stg_claims') }}
),
customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
)

SELECT
    c.claim_id,
    c.claim_date,
    c.amount,
    c.claim_type,
    c.is_fraud,
    cust.customer_id,
    cust.name AS customer_name,
    cust.state,
    cust.policy_type
FROM claims c
LEFT JOIN customers cust
    ON c.customer_id = cust.customer_id;
