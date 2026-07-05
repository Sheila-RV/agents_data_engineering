from datetime import date

import polars as pl

from lakekeeper.pipeline.quality import engine
from lakekeeper.pipeline.store import TableStore


def _eval_one(df: pl.DataFrame, rule: dict, store: TableStore | None = None):
    report = engine.evaluate(df, "test.t", [rule], store)
    return report.results[0]


def test_not_null_rule() -> None:
    df = pl.DataFrame({"a": ["x", None, "y", None]})
    r = _eval_one(df, {"id": "a_nn", "type": "not_null", "column": "a"})
    assert r.failed_rows == 2
    assert len(r.sample) == 2


def test_unique_rule() -> None:
    df = pl.DataFrame({"a": ["x", "x", "y"]})
    r = _eval_one(df, {"id": "a_uq", "type": "unique", "column": "a"})
    assert r.failed_rows == 2  # both copies of the duplicate fail


def test_allowed_values_passes_nulls() -> None:
    df = pl.DataFrame({"a": ["BOB", "XXX", None]})
    r = _eval_one(df, {"id": "a_av", "type": "allowed_values", "column": "a", "values": ["BOB"]})
    assert r.failed_rows == 1


def test_range_rule() -> None:
    df = pl.DataFrame({"a": [0.5, 10.0, -1.0, None]})
    r = _eval_one(df, {"id": "a_rg", "type": "range", "column": "a", "min": 0.0, "max": 5.0})
    assert r.failed_rows == 2  # 10.0 and -1.0; null passes


def test_fail_when_rule() -> None:
    df = pl.DataFrame({"amount": [5.0, -2.0, 0.0]})
    r = _eval_one(df, {"id": "pos", "type": "fail_when", "expr": "amount <= 0"})
    assert r.failed_rows == 2


def test_foreign_key_rule(store: TableStore) -> None:
    store.write("silver", "parents", pl.DataFrame({"id": ["P1", "P2"]}))
    df = pl.DataFrame({"pid": ["P1", "P9", None]})
    rule = {
        "id": "fk",
        "type": "foreign_key",
        "column": "pid",
        "ref": "silver.parents",
        "ref_column": "id",
    }
    r = _eval_one(df, rule, store)
    assert r.failed_rows == 1  # P9 orphan; null passes (not_null's job)


def test_quarantine_moves_error_rows(store: TableStore) -> None:
    df = pl.DataFrame(
        {
            "customer_id": ["C1", "C2", "C3"],
            "full_name": ["a", "b", "c"],
            "doc_id": ["1", None, "3"],
            "birth_date": [date(1990, 1, 1), date(1991, 1, 1), None],
            "city": ["La Paz"] * 3,
            "segment": ["retail"] * 3,
            "risk_rating": ["low"] * 3,
            "created_at": [date(2020, 1, 1)] * 3,
            "_run_id": ["r1"] * 3,
        }
    )
    store.write("silver", "customers", df)
    moved = engine.quarantine_failures(store, "silver", "customers")
    assert moved == 2
    assert store.read("silver", "customers").get_column("customer_id").to_list() == ["C1"]
    quarantined = store.read("quarantine", "customers").sort("customer_id")
    assert quarantined.get_column("customer_id").to_list() == ["C2", "C3"]
    reasons = dict(quarantined.select("customer_id", "_reject_reason").iter_rows())
    assert reasons["C2"] == "doc_id_not_null"
    assert reasons["C3"] == "birth_date_not_null"


def test_quarantine_is_idempotent(store: TableStore) -> None:
    df = pl.DataFrame(
        {
            "customer_id": ["C1", "C2"],
            "full_name": ["a", "b"],
            "doc_id": ["1", None],
            "birth_date": [date(1990, 1, 1)] * 2,
            "city": ["Sucre"] * 2,
            "segment": ["retail"] * 2,
            "risk_rating": ["low"] * 2,
            "created_at": [date(2020, 1, 1)] * 2,
            "_run_id": ["r1"] * 2,
        }
    )
    store.write("silver", "customers", df)
    engine.quarantine_failures(store, "silver", "customers")
    # Simulate a rerun: the merge re-inserts the bad row into silver.
    store.merge("silver", "customers", df, key="customer_id")
    engine.quarantine_failures(store, "silver", "customers")
    assert store.read("quarantine", "customers").height == 1
