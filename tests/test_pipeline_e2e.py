"""Walking-skeleton test: landing files -> bronze -> silver -> gold KPIs."""

from pathlib import Path

import pytest

from lakekeeper.config import Settings
from lakekeeper.pipeline import bronze
from lakekeeper.pipeline.runner import run_deterministic
from lakekeeper.pipeline.store import TableStore
from tests.conftest import RUN_DATE


@pytest.fixture()
def settings(tmp_path: Path, landing_dir: Path) -> Settings:
    return Settings(lake_root=tmp_path, _env_file=None)


def test_end_to_end_run(settings: Settings) -> None:
    summary = run_deterministic(settings, RUN_DATE)
    store = TableStore(settings.lake_dir)

    assert {r.table for r in summary.ingested} == {
        "customers",
        "accounts",
        "transactions",
        "fx_rates",
    }
    assert not any(r.has_drift for r in summary.ingested)

    # Clean data: every DQ report passes, nothing quarantined, recon is green.
    assert all(r.passed for r in summary.dq_reports)
    assert all(t.quarantined == 0 for t in summary.transformed)
    assert summary.reconciliation is not None and summary.reconciliation.ok

    txns = store.read("silver", "transactions")
    assert txns.height == 200
    assert txns["txn_id"].n_unique() == 200
    assert txns["amount_bob"].null_count() == 0
    # USD transactions must be converted at ~6.96 BOB/USD.
    usd = txns.filter(txns["currency"] == "USD")
    if usd.height:
        ratio = (usd["amount_bob"] / usd["amount"]).round(2)
        assert set(ratio.unique()) == {6.96}

    # Full star schema materialized.
    assert set(summary.gold_models) == {
        "dim_account",
        "dim_customer",
        "dim_date",
        "fact_transactions",
        "kpi_daily",
    }
    fact = store.read("gold", "fact_transactions")
    assert fact.height == 200
    assert fact["customer_id"].null_count() == 0
    assert store.read("gold", "dim_date").height >= 1

    kpi = store.read("gold", "kpi_daily")
    assert kpi.height == 1
    assert kpi["txn_count"][0] == 200
    assert kpi["volume_bob"][0] > 0
    assert kpi["quarantine_rate_pct"][0] == 0.0


def test_rerun_is_idempotent(settings: Settings) -> None:
    run_deterministic(settings, RUN_DATE)
    run_deterministic(settings, RUN_DATE)  # ingests bronze twice; silver dedupes
    store = TableStore(settings.lake_dir)
    assert store.read("bronze", "transactions").height == 400
    assert store.read("silver", "transactions").height == 200
    assert store.read("gold", "kpi_daily")["txn_count"][0] == 200


def test_bad_rows_are_quarantined(settings: Settings, landing_dir: Path) -> None:
    # Corrupt the drop: a transaction with a negative amount and one on an
    # account that does not exist.
    txn_file = landing_dir / "transactions_20260701.jsonl"
    good_line = txn_file.read_text(encoding="utf-8").splitlines()[0]
    import json

    bad_neg = json.loads(good_line)
    bad_neg.update(txn_id="TXN-BAD-NEG", amount=-50.0)
    bad_orphan = json.loads(good_line)
    bad_orphan.update(txn_id="TXN-BAD-ORPHAN", account_id="ACC-999999")
    with txn_file.open("a", encoding="utf-8") as f:
        f.write(json.dumps(bad_neg) + "\n")
        f.write(json.dumps(bad_orphan) + "\n")

    summary = run_deterministic(settings, RUN_DATE)
    store = TableStore(settings.lake_dir)

    quarantined = store.read("quarantine", "transactions")
    reasons = dict(quarantined.select("txn_id", "_reject_reason").iter_rows())
    assert reasons["TXN-BAD-NEG"] == "amount_positive"
    assert reasons["TXN-BAD-ORPHAN"] == "account_exists"
    assert store.read("silver", "transactions").height == 200  # clean rows only
    assert summary.reconciliation is not None and summary.reconciliation.ok


def test_schema_drift_detected(settings: Settings, landing_dir: Path) -> None:
    bad = landing_dir / "transactions_20260702.jsonl"
    bad.write_text('{"txn_id": "TXN-X", "amt": "12.5"}\n', encoding="utf-8")
    result = bronze.inspect_file(bad)
    assert result.has_drift
    assert "amount" in result.missing_columns
    assert "amt" in result.unexpected_columns
    with pytest.raises(bronze.SchemaDriftError):
        bronze.ingest_file(TableStore(settings.lake_dir), bad, "run-test")
