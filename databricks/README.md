# Databricks artifacts (illustrative)

These files show how the local pipeline maps onto Databricks — they mirror the local
code, they are **not executed by CI** and are not required to run the project. See
[docs/databricks_mapping.md](../docs/databricks_mapping.md) for the full concept map.

- `notebooks/01_bronze_autoloader.py` — Auto Loader ingestion ≙ local `pipeline/bronze.py`
- `notebooks/02_silver_transform.py` — Lakeflow declarative pipeline with expectations
  ≙ local `pipeline/silver.py` + `pipeline/quality/`
- `notebooks/03_gold_star_schema.sql` — Databricks SQL gold models ≙ local `pipeline/sql/`
- `resources/lakekeeper_job.yml` — Asset Bundle job definition for the bronze→silver→gold
  DAG with retries ≙ the supervisor's happy-path plan
