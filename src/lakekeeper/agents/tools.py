"""Boundary between the agents and the deterministic pipeline.

Agent nodes only touch the lake through these functions (thin wrappers over
pipeline code). The dependency is one-way: `pipeline/` never imports from
`agents/`.
"""

from pathlib import Path

from lakekeeper.config import Settings
from lakekeeper.pipeline import bronze, gold
from lakekeeper.pipeline.quality import engine as quality
from lakekeeper.pipeline.reconciliation import reconcile
from lakekeeper.pipeline.runner import build_silver_step
from lakekeeper.pipeline.store import TableStore


def list_landing_files(settings: Settings, run_date_stamp: str) -> list[Path]:
    return bronze.list_landing_files(settings.landing_dir, run_date_stamp)


def inspect_landing_file(path: Path) -> bronze.IngestResult:
    return bronze.inspect_file(path)


def ingest_to_bronze(
    store: TableStore, path: Path, run_id: str, *, allow_drift: bool = False
) -> bronze.IngestResult:
    return bronze.ingest_file(store, path, run_id, allow_drift=allow_drift)


def run_silver(store: TableStore, table: str):
    return build_silver_step(store, table)


def run_gold_models(store: TableStore) -> dict[str, int]:
    return {model: gold.run_model(store, model) for model in gold.available_models()}


def run_dq_rules(store: TableStore, table: str) -> quality.DQReport:
    return quality.evaluate_table(store, "silver", table)


def quarantine_records(store: TableStore, table: str, rule_ids: list[str]) -> int:
    return quality.quarantine_failures(store, "silver", table, rule_ids=rule_ids)


def ensure_quarantine(store: TableStore, table: str) -> None:
    quality.ensure_quarantine_table(store, table, store.read("silver", table))


def reconcile_all(store: TableStore):
    return reconcile(store)
