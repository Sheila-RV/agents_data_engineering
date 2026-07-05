"""Walking-skeleton test: landing files -> bronze -> silver -> gold KPIs."""

from pathlib import Path

import pytest
from tests.conftest import RUN_DATE

from lakekeeper.config import Settings
from lakekeeper.pipeline import bronze
from lakekeeper.pipeline.runner import run_deterministic
from lakekeeper.pipeline.store import TableStore


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

    txns = store.read("silver", "transactions")
    assert txns.height == 200
    assert txns["txn_id"].n_unique() == 200
    assert txns["amount_bob"].null_count() == 0
    # USD transactions must be converted at ~6.96 BOB/USD.
    usd = txns.filter(txns["currency"] == "USD")
    if usd.height:
        ratio = (usd["amount_bob"] / usd["amount"]).round(2)
        assert set(ratio.unique()) == {6.96}

    kpi = store.read("gold", "kpi_daily")
    assert kpi.height == 1
    assert kpi["txn_count"][0] == 200
    assert kpi["volume_bob"][0] > 0


def test_rerun_is_idempotent(settings: Settings) -> None:
    run_deterministic(settings, RUN_DATE)
    run_deterministic(settings, RUN_DATE)  # ingests bronze twice; silver dedupes
    store = TableStore(settings.lake_dir)
    assert store.read("bronze", "transactions").height == 400
    assert store.read("silver", "transactions").height == 200
    assert store.read("gold", "kpi_daily")["txn_count"][0] == 200


def test_schema_drift_detected(settings: Settings, landing_dir: Path) -> None:
    bad = landing_dir / "transactions_20260702.jsonl"
    bad.write_text('{"txn_id": "TXN-X", "amt": "12.5"}\n', encoding="utf-8")
    result = bronze.inspect_file(bad)
    assert result.has_drift
    assert "amount" in result.missing_columns
    assert "amt" in result.unexpected_columns
    with pytest.raises(bronze.SchemaDriftError):
        bronze.ingest_file(TableStore(settings.lake_dir), bad, "run-test")
