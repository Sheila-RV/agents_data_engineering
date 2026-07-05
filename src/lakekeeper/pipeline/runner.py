"""Deterministic end-to-end pipeline run (no agents).

This is the walking skeleton the agent layer later supervises: the same
functions, called in fixed order, failing fast on any problem.
"""

import uuid
from dataclasses import dataclass, field
from datetime import date

from lakekeeper.config import Settings
from lakekeeper.pipeline import bronze, gold, silver
from lakekeeper.pipeline.store import TableStore


@dataclass
class RunSummary:
    run_id: str
    run_date: date
    ingested: list[bronze.IngestResult] = field(default_factory=list)
    transformed: list[silver.TransformResult] = field(default_factory=list)
    gold_models: dict[str, int] = field(default_factory=dict)


def new_run_id(run_date: date) -> str:
    return f"run-{run_date:%Y%m%d}-{uuid.uuid4().hex[:8]}"


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

    summary.transformed.append(silver.build_fx_rates(store))
    summary.transformed.append(silver.build_transactions(store))

    for model in gold.available_models():
        summary.gold_models[model] = gold.run_model(store, model)
    return summary
