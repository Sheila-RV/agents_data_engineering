# Databricks mapping

Lakekeeper runs locally on purpose (anyone can clone and run it for free), but it is
engineered so every concept maps one-to-one onto Databricks. This document is that map;
the [databricks/](../databricks/) folder contains illustrative notebooks and an Asset
Bundle job definition for the same pipeline.

## Concept map

| Local (this repo) | Databricks equivalent |
|---|---|
| `data/lake/<layer>/<table>` Delta directories (delta-rs) | Unity Catalog managed Delta tables: `lakekeeper.<layer>.<table>` |
| `TableStore.write(mode="append")` | `INSERT INTO` / streaming append |
| `TableStore.merge(key=...)` ([store.py](../src/lakekeeper/pipeline/store.py)) | `MERGE INTO target USING source ON s.key = t.key WHEN MATCHED UPDATE ... WHEN NOT MATCHED INSERT ...` |
| Landing dir + [bronze.py](../src/lakekeeper/pipeline/bronze.py) (verbatim strings + `_ingested_at`, `_source_file`, `_run_id`) | Auto Loader (`cloudFiles`) into bronze streaming tables with `_metadata.file_name` and `current_timestamp()` |
| Schema contracts in `bronze.CONTRACTS` + `SchemaDriftError` | Auto Loader `schemaEvolutionMode: rescue` + schema hints; drift lands in `_rescued_data` |
| Polars silver builders ([silver.py](../src/lakekeeper/pipeline/silver.py)) | PySpark DataFrame API - see [02_silver_transform.py](../databricks/notebooks/02_silver_transform.py) |
| Gold SQL models ([pipeline/sql/](../src/lakekeeper/pipeline/sql/)) run by DuckDB | Databricks SQL / Lakeflow Spark Declarative Pipelines materialized views - near copy-paste |
| YAML DQ rules + engine ([quality/](../src/lakekeeper/pipeline/quality/)) | Lakeflow expectations (`@dlt.expect_or_drop`, `expect_or_fail`) or DQX rule sets |
| Quarantine tables with `_reject_reason` | The standard expectations-quarantine pattern: route dropped rows to a quarantine table with the violated expectation |
| Supervisor retry/skip/abort on step failure | Lakeflow Jobs task retries + repair-run; the agent layer itself runs anywhere with workspace API access |
| `run_log_*.json` ledger + `report_*.md` | Job run output, event log, system tables (`system.lakeflow`) |
| `reconciliation.py` checks | A validation task/notebook at the end of the job DAG, or DQX comparison rules |
| Fixed-seed `datagen` | A fixture notebook or dbldatagen |

## SQL portability example

[`kpi_daily.sql`](../src/lakekeeper/pipeline/sql/kpi_daily.sql) runs unchanged on
Databricks SQL except for table names:

```sql
-- local (DuckDB over registered views)     -- Databricks
FROM silver_transactions                     FROM lakekeeper.silver.transactions
```

`FILTER (WHERE ...)` aggregates, `generate_series` for `dim_date`, and window functions
are supported by both engines.

## Honest gaps

delta-rs and Spark Delta are not feature-identical. Lakekeeper deliberately sticks to the
common subset - plain append/overwrite/merge, no deletion vectors, no liquid clustering,
no `CDF`. Time travel exists on both but the retention/vacuum story differs. If you take
these tables to Databricks, run `CONVERT TO DELTA`-era features (predictive optimization,
liquid clustering) from the Databricks side.

The agent layer is platform-agnostic: point [tools.py](../src/lakekeeper/agents/tools.py)
at Databricks SQL warehouses / Jobs APIs instead of the local TableStore and the same
LangGraph supervision loop operates a Databricks pipeline.
