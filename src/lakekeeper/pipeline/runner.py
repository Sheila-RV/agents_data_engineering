"""Deterministic end-to-end pipeline run (no agents).

This is the exact sequence the agent layer later supervises — same functions,
fixed order, default quality policy (quarantine every error-severity failure),
failing fast on anything unexpected.
"""

import uuid
from dataclasses import dataclass, field
from datetime import date

from lakekeeper.config import Settings
from lakekeeper.pipeline import bronze, gold, silver
from lakekeeper.pipeline.quality import engine as quality
from lakekeeper.pipeline.reconciliation import ReconciliationResult, reconcile
from lakekeeper.pipeline.store import TableStore

# Silver build order matters: foreign-key rules check against already-built
# (and already-quarantined) parents.
SILVER_STEPS = ["fx_rates", "customers", "accounts", "transactions"]


@dataclass
class RunSummary:
    run_id: str
    run_date: date
    ingested: list[bronze.IngestResult] = field(default_factory=list)
    transformed: list[silver.TransformResult] = field(default_factory=list)
    dq_reports: list[quality.DQReport] = field(default_factory=list)
    gold_models: dict[str, int] = field(default_factory=dict)
    reconciliation: ReconciliationResult | None = None


def new_run_id(run_date: date) -> str:
    return f"run-{run_date:%Y%m%d}-{uuid.uuid4().hex[:8]}"


def build_silver_step(store: TableStore, table: str) -> silver.TransformResult:
    builder = getattr(silver, f"build_{table}")
    return builder(store)


def apply_quality(store: TableStore, table: str) -> tuple[quality.DQReport, int]:
    """Evaluate rules on a silver table and apply the default policy:
    quarantine every error-severity failure. Returns (report, rows quarantined)."""
    report = quality.evaluate_table(store, "silver", table)
    quarantined = 0
    if not report.passed:
        quarantined = quality.quarantine_failures(store, "silver", table)
    else:
        quality.ensure_quarantine_table(store, table, store.read("silver", table))
    return report, quarantined


def run_deterministic(settings: Settings, run_date: date) -> RunSummary:
    store = TableStore(settings.lake_dir)
    summary = RunSummary(run_id=new_run_id(run_date), run_date=run_date)

    files = bronze.list_landing_files(settings.landing_dir, f"{run_date:%Y%m%d}")
    if not files:
        raise FileNotFoundError(
            f"no landing files for {run_date} in {settings.landing_dir} — "
            "run `lakekeeper generate` first"
        )
    for path in files:
        summary.ingested.append(bronze.ingest_file(store, path, summary.run_id))

    for table in SILVER_STEPS:
        result = build_silver_step(store, table)
        if quality.load_rules("silver", table):
            report, result.quarantined = apply_quality(store, table)
            summary.dq_reports.append(report)
        summary.transformed.append(result)

    for model in gold.available_models():
        summary.gold_models[model] = gold.run_model(store, model)

    summary.reconciliation = reconcile(store)
    return summary
