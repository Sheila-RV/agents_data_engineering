-- Transaction fact table. Grain: one row per transaction.
-- FKs: date_key -> dim_date, account_id -> dim_account, customer_id -> dim_customer.
SELECT
    t.txn_id,
    t.event_date        AS date_key,
    t.account_id,
    a.customer_id,
    t.amount            AS amount_orig,
    t.currency,
    t.amount_bob,
    t.txn_type,
    t.channel,
    t.merchant_category,
    t.is_flagged,
    t.is_late
FROM silver_transactions t
LEFT JOIN silver_accounts a USING (account_id)
ORDER BY t.txn_id
