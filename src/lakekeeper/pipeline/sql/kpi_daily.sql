-- Daily banking KPIs over the silver transaction ledger.
-- Runs on DuckDB locally; translates near-verbatim to Databricks SQL.
WITH q AS (
    SELECT COUNT(*) AS quarantined FROM quarantine_transactions
)
SELECT
    t.event_date,
    COUNT(*)                                                        AS txn_count,
    ROUND(SUM(t.amount_bob), 2)                                     AS volume_bob,
    ROUND(AVG(t.amount_bob), 2)                                     AS avg_txn_bob,
    COUNT(DISTINCT t.account_id)                                    AS active_accounts,
    ROUND(100.0 * COUNT(*) FILTER (WHERE t.is_flagged) / COUNT(*), 2) AS fraud_flag_rate_pct,
    COUNT(*) FILTER (WHERE t.is_late)                               AS late_txns,
    COUNT(*) FILTER (WHERE t.channel = 'ATM')                       AS atm_txns,
    COUNT(*) FILTER (WHERE t.channel = 'POS')                       AS pos_txns,
    COUNT(*) FILTER (WHERE t.channel = 'web')                       AS web_txns,
    COUNT(*) FILTER (WHERE t.channel = 'branch')                    AS branch_txns,
    ROUND(100.0 * q.quarantined / (q.quarantined + (SELECT COUNT(*) FROM silver_transactions)), 2)
                                                                    AS quarantine_rate_pct
FROM silver_transactions t, q
GROUP BY t.event_date, q.quarantined
ORDER BY t.event_date
