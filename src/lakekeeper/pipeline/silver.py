"""Silver layer: typed, deduplicated, conformed tables built from bronze.

All transforms are pure Polars over bronze inputs and are idempotent: re-running
a date produces the same silver state (dedupe keeps the latest ingested copy,
writes are MERGE upserts on the business key). Casts use strict=False so bad
values become nulls - the data-quality rules then catch and quarantine them
instead of the cast blowing up mid-pipeline.
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


def _latest(df: pl.DataFrame, key: str) -> pl.DataFrame:
    return df.sort("_ingested_at").unique(subset=[key], keep="last")


def build_fx_rates(store: TableStore) -> TransformResult:
    bronze = store.read("bronze", "fx_rates")
    silver = (
        bronze.sort("_ingested_at")
        .unique(subset=["rate_date", "currency"], keep="last")
        .select(
            pl.col("rate_date").str.to_date(strict=False),
            pl.col("currency"),
            pl.col("rate_to_bob").cast(pl.Float64, strict=False),
        )
        .sort("rate_date", "currency")
    )
    store.write("silver", "fx_rates", silver, mode="overwrite")
    return TransformResult("fx_rates", bronze.height, silver.height)


def build_customers(store: TableStore) -> TransformResult:
    bronze = store.read("bronze", "customers")
    silver = _latest(bronze, "customer_id").select(
        pl.col("customer_id"),
        pl.col("full_name").str.strip_chars(),
        pl.col("doc_id"),
        pl.col("birth_date").str.to_date(strict=False),
        pl.col("city"),
        pl.col("segment").str.to_lowercase(),
        pl.col("risk_rating").str.to_lowercase(),
        pl.col("created_at").str.to_date(strict=False),
        pl.col("_run_id"),
    )
    store.merge("silver", "customers", silver, key="customer_id")
    return TransformResult("customers", bronze.height, silver.height)


def build_accounts(store: TableStore) -> TransformResult:
    bronze = store.read("bronze", "accounts")
    silver = _latest(bronze, "account_id").select(
        pl.col("account_id"),
        pl.col("customer_id"),
        pl.col("account_type").str.to_lowercase(),
        pl.col("currency"),
        pl.col("opened_at").str.to_date(strict=False),
        pl.col("status").str.to_lowercase(),
        pl.col("_run_id"),
    )
    store.merge("silver", "accounts", silver, key="account_id")
    return TransformResult("accounts", bronze.height, silver.height)


def build_transactions(store: TableStore) -> TransformResult:
    bronze = store.read("bronze", "transactions")
    typed = (
        _latest(bronze, "txn_id")
        .select(
            pl.col("txn_id"),
            pl.col("account_id"),
            pl.col("ts").str.to_datetime(time_unit="us", strict=False),
            pl.col("amount").cast(pl.Float64, strict=False),
            pl.col("currency"),
            pl.col("txn_type"),
            pl.col("channel"),
            pl.col("counterparty"),
            pl.col("merchant_category"),
            pl.col("is_flagged").str.to_lowercase() == "true",
            pl.col("_source_file"),
            pl.col("_run_id"),
        )
        .with_columns(
            pl.col("ts").dt.date().alias("event_date"),
            # A record is "late" when its event predates the landing file that
            # delivered it (file stamp is the delivery date).
            pl.col("_source_file")
            .str.extract(r"(\d{8})")
            .str.to_date("%Y%m%d")
            .alias("_file_date"),
        )
        .with_columns((pl.col("event_date") < pl.col("_file_date")).alias("is_late"))
        .drop("_source_file", "_file_date")
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
