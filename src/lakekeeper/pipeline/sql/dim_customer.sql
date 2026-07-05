-- Customer dimension (SCD Type 1: latest attributes win).
SELECT
    customer_id,
    full_name,
    doc_id,
    birth_date,
    city,
    segment,
    risk_rating,
    created_at AS customer_since
FROM silver_customers
ORDER BY customer_id
