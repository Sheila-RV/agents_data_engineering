"""Chaos profiles: seeded injection + the full agent run over messy data."""

import json
from pathlib import Path

import polars as pl
import pytest
from tests.conftest import RUN_DATE

from lakekeeper.agents.graph import run_with_agents
from lakekeeper.config import Settings
from lakekeeper.datagen import generate_landing_files
from lakekeeper.pipeline.store import TableStore


@pytest.fixture()
def chaos_settings(tmp_path: Path) -> Settings:
    generate_landing_files(
        RUN_DATE, tmp_path / "landing", seed=7, n_customers=20, n_transactions=200, chaos="high"
    )
    return Settings(lake_root=tmp_path, lakekeeper_mock_llm=True, _env_file=None)


def test_chaos_injects_expected_issues(tmp_path: Path) -> None:
    landing = tmp_path / "landing"
    generate_landing_files(
        RUN_DATE, landing, seed=7, n_customers=20, n_transactions=200, chaos="high"
    )

    txns = pl.read_ndjson(landing / "transactions_20260701.jsonl")
    assert txns.height > 200  # duplicates appended
    assert txns["txn_id"].n_unique() < txns.height
    assert txns["amount"].null_count() >= 1
    assert txns.filter(pl.col("ts").str.to_datetime().dt.date() < RUN_DATE).height >= 1

    customers = pl.read_csv(landing / "customers_20260701.csv")
    assert customers["doc_id"].null_count() >= 1

    accounts = pl.read_csv(landing / "accounts_20260701.csv")
    assert "estado" in accounts.columns and "status" not in accounts.columns

    fx = json.loads((landing / "fx_rates_20260701.json").read_text(encoding="utf-8"))
    assert "USD" not in fx["rates_to_bob"]


def test_chaos_is_reproducible(tmp_path: Path) -> None:
    a, b = tmp_path / "a", tmp_path / "b"
    for d in (a, b):
        generate_landing_files(
            RUN_DATE, d, seed=7, n_customers=20, n_transactions=200, chaos="high"
        )
    name = "transactions_20260701.jsonl"
    assert (a / name).read_bytes() == (b / name).read_bytes()


def test_agents_operate_a_chaos_high_run(chaos_settings: Settings) -> None:
    final = run_with_agents(chaos_settings, RUN_DATE)
    assert final["status"] == "done"

    agents_involved = {d["agent"] for d in final["decisions"]}
    assert "ingestion" in agents_involved  # schema drift on accounts
    assert "quality" in agents_involved  # null amounts / doc_ids
    assert "validation" in agents_involved  # fraud-rate spike

    # Drift: accounts ingested aligned, estado dropped, status null.
    drift = next(d for d in final["decisions"] if d["agent"] == "ingestion")
    assert drift["decision"]["action"] == "ingest_aligned"
    assert drift["context"]["missing_columns"] == ["status"]
    assert drift["context"]["unexpected_columns"] == ["estado"]

    # Quality: bad rows quarantined, run kept going.
    assert final["quarantined_total"] >= 2
    store = TableStore(chaos_settings.lake_dir)
    assert store.read("quarantine", "transactions").height >= 1
    assert store.read("quarantine", "customers").height >= 1

    # Validation: fraud spike breaches the baseline -> pass_with_warnings.
    verdict = final["verdict"]
    assert verdict["verdict"] == "pass_with_warnings"
    assert "fraud" in verdict["explanation"]

    # Warn-level findings (late records, missing USD rate) are in the DQ reports.
    txn_report = next(r for r in final["dq_reports"] if r["table"] == "silver.transactions")
    warn_failed = {
        r["rule_id"] for r in txn_report["results"] if r["severity"] == "warn" and r["failed_rows"]
    }
    assert "late_arrivals" in warn_failed
    assert "fx_rate_missing" in warn_failed

    # Everything is explained in the report.
    assert final["report_md"] is not None
