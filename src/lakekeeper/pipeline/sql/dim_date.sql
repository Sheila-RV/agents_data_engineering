-- Calendar dimension spanning the transaction ledger.
SELECT
    CAST(gs.d AS DATE)                       AS date_key,
    EXTRACT(year FROM gs.d)                  AS year,
    EXTRACT(quarter FROM gs.d)               AS quarter,
    EXTRACT(month FROM gs.d)                 AS month,
    strftime(gs.d, '%B')                     AS month_name,
    EXTRACT(day FROM gs.d)                   AS day,
    EXTRACT(isodow FROM gs.d)                AS iso_weekday,
    strftime(gs.d, '%A')                     AS weekday_name,
    EXTRACT(isodow FROM gs.d) IN (6, 7)      AS is_weekend
FROM generate_series(
    (SELECT MIN(event_date)::TIMESTAMP FROM silver_transactions),
    (SELECT MAX(event_date)::TIMESTAMP FROM silver_transactions),
    INTERVAL 1 DAY
) AS gs(d)
ORDER BY date_key
