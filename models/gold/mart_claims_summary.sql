WITH enriched AS (
    SELECT * FROM {{ ref('fct_claims_enriched') }}
)

SELECT
    state,
    policy_type,
    COUNT(claim_id) AS total_claims,
    SUM(amount) AS total_amount,
    SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) AS fraud_cases,
    ROUND(SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) AS fraud_rate
FROM enriched
GROUP BY state, policy_type
ORDER BY fraud_rate DESC;
