"""TableStore: the single gateway to the local Delta lake.

Every layer (bronze/silver/gold/quarantine) reads and writes through this class,
which keeps the engine details (delta-rs + Polars + DuckDB) in one place. On
Databricks the same operations map to Unity Catalog tables: `write(mode="append")`
-> INSERT, `merge()` -> MERGE INTO, `sql()` -> a Databricks SQL query
(see docs/databricks_mapping.md).
"""

from pathlib import Path

import duckdb
import polars as pl
from deltalake import DeltaTable

LAYERS = ("bronze", "silver", "gold", "quarantine")


class TableStore:
    def __init__(self, lake_dir: Path) -> None:
        self.lake_dir = lake_dir

    def path(self, layer: str, table: str) -> Path:
        if layer not in LAYERS:
            raise ValueError(f"unknown layer {layer!r}; expected one of {LAYERS}")
        return self.lake_dir / layer / table

    def exists(self, layer: str, table: str) -> bool:
        return (self.path(layer, table) / "_delta_log").exists()

    def list_tables(self, layer: str) -> list[str]:
        layer_dir = self.lake_dir / layer
        if not layer_dir.exists():
            return []
        return sorted(p.name for p in layer_dir.iterdir() if (p / "_delta_log").exists())

    def read(self, layer: str, table: str) -> pl.DataFrame:
        # Read via deltalake + pyarrow rather than pl.read_delta: polars' native
        # delta scan percent-encodes spaces in Windows paths and then fails to
        # find its own parquet files.
        dt = DeltaTable(str(self.path(layer, table)))
        return pl.from_arrow(dt.to_pyarrow_table())

    def write(self, layer: str, table: str, df: pl.DataFrame, *, mode: str = "append") -> None:
        """mode: 'append' | 'overwrite' (overwrite also replaces the schema)."""
        target = self.path(layer, table)
        target.parent.mkdir(parents=True, exist_ok=True)
        options = {"schema_mode": "overwrite"} if mode == "overwrite" else {}
        df.write_delta(str(target), mode=mode, delta_write_options=options)

    def merge(self, layer: str, table: str, df: pl.DataFrame, *, key: str | list[str]) -> None:
        """Upsert on a business key - the local equivalent of Databricks MERGE INTO."""
        if not self.exists(layer, table):
            self.write(layer, table, df, mode="append")
            return
        keys = [key] if isinstance(key, str) else key
        predicate = " AND ".join(f"s.{k} = t.{k}" for k in keys)
        (
            df.write_delta(
                str(self.path(layer, table)),
                mode="merge",
                delta_merge_options={
                    "predicate": predicate,
                    "source_alias": "s",
                    "target_alias": "t",
                },
            )
            .when_matched_update_all()
            .when_not_matched_insert_all()
            .execute()
        )

    def sql(self, query: str) -> pl.DataFrame:
        """Run SQL over the lake with DuckDB.

        Every existing table is registered as `<layer>_<table>` (e.g.
        `silver_transactions`), mirroring `<schema>.<table>` names on Databricks.
        """
        con = duckdb.connect()
        try:
            for layer in LAYERS:
                for table in self.list_tables(layer):
                    con.register(f"{layer}_{table}", self.read(layer, table))
            return con.execute(query).pl()
        finally:
            con.close()
