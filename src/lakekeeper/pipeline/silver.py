"""Silver layer: typed, deduplicated, conformed tables built from bronze.

All transforms are pure Polars over bronze inputs and are idempotent: re-running
a date produces the same silver state (dedupe keeps the latest ingested copy,
writes are MERGE upserts on the business key).
"""

from dataclasses import dataclass

import polars as pl

from lakekeeper.pipeline.store import TableStore


@dataclass
class TransformResult:
    table: str
    rows_in: int
    rows_out: int
    quarantined: int = 0


def build_fx_rates(store: TableStore) -> TransformResult:
    bronze = store.read("bronze", "fx_rates")
    silver = (
        bronze.sort("_ingested_at")
        .unique(subset=["rate_date", "currency"], keep="last")
        .select(
            pl.col("rate_date").str.to_date(),
            pl.col("currency"),
            pl.col("rate_to_bob").cast(pl.Float64),
        )
        .sort("rate_date", "currency")
    )
    store.write("silver", "fx_rates", silver, mode="overwrite")
    return TransformResult("fx_rates", bronze.height, silver.height)


def build_transactions(store: TableStore) -> TransformResult:
    bronze = store.read("bronze", "transactions")
    typed = (
        bronze.sort("_ingested_at")
        .unique(subset=["txn_id"], keep="last")
        .select(
            pl.col("txn_id"),
            pl.col("account_id"),
            pl.col("ts").str.to_datetime(time_unit="us"),
            pl.col("amount").cast(pl.Float64),
            pl.col("currency"),
            pl.col("txn_type"),
            pl.col("channel"),
            pl.col("counterparty"),
            pl.col("merchant_category"),
            pl.col("is_flagged").str.to_lowercase() == "true",
            pl.col("_run_id"),
        )
        .with_columns(pl.col("ts").dt.date().alias("event_date"))
    )

    fx = store.read("silver", "fx_rates")
    converted = (
        typed.join(
            fx.rename({"rate_date": "event_date"}),
            on=["event_date", "currency"],
            how="left",
        )
        # Late-arriving records may predate the earliest FX file: fall back to the
        # most recent known rate for that currency.
        .with_columns(
            pl.col("rate_to_bob").fill_null(
                pl.col("rate_to_bob").last().over("currency", order_by="event_date")
            )
        )
        .with_columns((pl.col("amount") * pl.col("rate_to_bob")).round(2).alias("amount_bob"))
        .drop("rate_to_bob")
    )

    store.merge("silver", "transactions", converted, key="txn_id")
    return TransformResult("transactions", bronze.height, converted.height)
