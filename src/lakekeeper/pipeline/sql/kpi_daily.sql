-- Daily banking KPIs over the silver transaction ledger.
-- Runs on DuckDB locally; translates near-verbatim to Databricks SQL.
SELECT
    event_date,
    COUNT(*)                                                    AS txn_count,
    ROUND(SUM(amount_bob), 2)                                   AS volume_bob,
    ROUND(AVG(amount_bob), 2)                                   AS avg_txn_bob,
    COUNT(DISTINCT account_id)                                  AS active_accounts,
    ROUND(100.0 * COUNT(*) FILTER (WHERE is_flagged) / COUNT(*), 2) AS fraud_flag_rate_pct,
    COUNT(*) FILTER (WHERE channel = 'ATM')                     AS atm_txns,
    COUNT(*) FILTER (WHERE channel = 'POS')                     AS pos_txns,
    COUNT(*) FILTER (WHERE channel = 'web')                     AS web_txns,
    COUNT(*) FILTER (WHERE channel = 'branch')                  AS branch_txns
FROM silver_transactions
GROUP BY event_date
ORDER BY event_date
