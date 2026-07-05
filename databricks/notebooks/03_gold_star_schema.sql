-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Gold star schema — Databricks SQL
-- MAGIC Near-verbatim copies of the local `pipeline/sql/*.sql` DuckDB models.

-- COMMAND ----------
CREATE OR REPLACE MATERIALIZED VIEW lakekeeper.gold.fact_transactions AS
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
FROM lakekeeper.silver.transactions t
LEFT JOIN lakekeeper.silver.accounts a USING (account_id);

-- COMMAND ----------
CREATE OR REPLACE MATERIALIZED VIEW lakekeeper.gold.kpi_daily AS
SELECT
    t.event_date,
    COUNT(*)                                                          AS txn_count,
    ROUND(SUM(t.amount_bob), 2)                                       AS volume_bob,
    ROUND(AVG(t.amount_bob), 2)                                       AS avg_txn_bob,
    COUNT(DISTINCT t.account_id)                                      AS active_accounts,
    ROUND(100.0 * COUNT(*) FILTER (WHERE t.is_flagged) / COUNT(*), 2) AS fraud_flag_rate_pct,
    COUNT(*) FILTER (WHERE t.is_late)                                 AS late_txns,
    COUNT(*) FILTER (WHERE t.channel = 'ATM')                         AS atm_txns,
    COUNT(*) FILTER (WHERE t.channel = 'POS')                         AS pos_txns,
    COUNT(*) FILTER (WHERE t.channel = 'web')                         AS web_txns,
    COUNT(*) FILTER (WHERE t.channel = 'branch')                      AS branch_txns
FROM lakekeeper.silver.transactions t
GROUP BY t.event_date;
