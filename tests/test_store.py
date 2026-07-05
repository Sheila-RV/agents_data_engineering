import polars as pl
import pytest

from lakekeeper.pipeline.store import TableStore


def test_write_read_roundtrip(store: TableStore) -> None:
    df = pl.DataFrame({"id": [1, 2], "name": ["a", "b"]})
    store.write("bronze", "t", df)
    assert store.read("bronze", "t").sort("id").equals(df)
    assert store.exists("bronze", "t")
    assert store.list_tables("bronze") == ["t"]


def test_merge_upserts_on_key(store: TableStore) -> None:
    store.write("silver", "t", pl.DataFrame({"id": [1, 2], "v": ["old", "keep"]}))
    store.merge("silver", "t", pl.DataFrame({"id": [1, 3], "v": ["new", "insert"]}), key="id")
    result = dict(store.read("silver", "t").sort("id").iter_rows())
    assert result == {1: "new", 2: "keep", 3: "insert"}


def test_merge_creates_table_if_missing(store: TableStore) -> None:
    store.merge("silver", "t", pl.DataFrame({"id": [1], "v": ["x"]}), key="id")
    assert store.read("silver", "t").height == 1


def test_sql_registers_layer_prefixed_views(store: TableStore) -> None:
    store.write("silver", "numbers", pl.DataFrame({"n": [1, 2, 3]}))
    out = store.sql("SELECT SUM(n) AS total FROM silver_numbers")
    assert out["total"][0] == 6


def test_unknown_layer_rejected(store: TableStore) -> None:
    with pytest.raises(ValueError, match="unknown layer"):
        store.path("platinum", "t")
