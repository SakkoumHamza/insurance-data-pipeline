SELECT
    date::date AS claim_date,
    amount::float AS amount,
    claim_id,
    is_fraud::boolean AS is_fraud,
    claim_type,
    customer_id
FROM {{ source('insurance', 'claims') }}
