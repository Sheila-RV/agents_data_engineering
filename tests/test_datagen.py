import json
from pathlib import Path

import polars as pl
from tests.conftest import RUN_DATE

from lakekeeper.datagen import generate_landing_files


def test_generates_four_landing_files(landing_dir: Path) -> None:
    names = sorted(p.name for p in landing_dir.iterdir())
    assert names == [
        "accounts_20260701.csv",
        "customers_20260701.csv",
        "fx_rates_20260701.json",
        "transactions_20260701.jsonl",
    ]


def test_same_seed_is_reproducible(tmp_path: Path) -> None:
    a, b = tmp_path / "a", tmp_path / "b"
    generate_landing_files(RUN_DATE, a, seed=7, n_customers=10, n_transactions=50)
    generate_landing_files(RUN_DATE, b, seed=7, n_customers=10, n_transactions=50)
    for name in ("customers_20260701.csv", "transactions_20260701.jsonl"):
        assert (a / name).read_bytes() == (b / name).read_bytes()


def test_transactions_reference_generated_accounts(landing_dir: Path) -> None:
    accounts = pl.read_csv(landing_dir / "accounts_20260701.csv")
    txns = pl.read_ndjson(landing_dir / "transactions_20260701.jsonl")
    assert txns.height == 200
    assert set(txns["account_id"]) <= set(accounts["account_id"])


def test_fx_file_covers_all_currencies(landing_dir: Path) -> None:
    payload = json.loads((landing_dir / "fx_rates_20260701.json").read_text(encoding="utf-8"))
    assert payload["rates_to_bob"]["BOB"] == 1.0
    assert {"BOB", "USD", "EUR"} <= set(payload["rates_to_bob"])
