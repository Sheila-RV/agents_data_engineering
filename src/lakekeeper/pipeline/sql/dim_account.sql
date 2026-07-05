-- Account dimension.
SELECT
    account_id,
    customer_id,
    account_type,
    currency,
    opened_at,
    status
FROM silver_accounts
ORDER BY account_id
