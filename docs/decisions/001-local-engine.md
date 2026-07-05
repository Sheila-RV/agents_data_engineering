# ADR 001 - Local engine: Polars + DuckDB + delta-rs (not PySpark)

**Status**: accepted

## Context

The pipeline must be runnable by anyone who clones the repo - free, fast, no cloud
account - while staying credible as a Databricks-shaped lakehouse.

## Decision

Use **Polars** for silver transforms, **DuckDB** for gold SQL, and **delta-rs**
(`deltalake`) for storage. No Spark locally.

## Rationale

- PySpark on Windows is notoriously fragile (JVM, winutils.exe, Hadoop paths); this
  project is developed on Windows and CI runs on Linux - both must work with `pip install`.
- delta-rs writes **real Delta tables**, so the storage format is identical to Databricks
  even though the compute engine differs. The portability claim lives in the format and
  the SQL, not in sharing an engine.
- Gold models are plain SQL files that translate near-verbatim to Databricks SQL - the
  one place engine dialects would hurt is fenced off.

## Consequences

- Silver code is Polars, not PySpark; the mapping doc and a mirror notebook cover the
  translation.
- Stick to the delta feature subset both implementations share (append/overwrite/merge);
  no deletion vectors or liquid clustering.
