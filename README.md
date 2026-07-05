# Lakekeeper

> A multi-agent system that **operates** — not just runs — an end-to-end medallion lakehouse
> (bronze → silver → gold) over synthetic core-banking data.

🚧 **Work in progress** — full README with architecture diagrams, quickstart and demo coming soon.

Deterministic Python/SQL transformations do the data work; LangGraph agents powered by Claude
decide what to run, diagnose failures, quarantine bad records, retry with fixes, and write
human-readable run reports. Runs 100% locally on Delta Lake tables (no cloud account, no JVM),
with a documented mapping to Databricks.
