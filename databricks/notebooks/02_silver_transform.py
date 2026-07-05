# Databricks notebook source
# MAGIC %md
# MAGIC # Silver layer as a Lakeflow declarative pipeline
# MAGIC Equivalent of local `pipeline/silver.py` + the YAML DQ rules in
# MAGIC `pipeline/quality/rules/`: expectations drop bad rows the way the local
# MAGIC quality agent quarantines them.

# COMMAND ----------
import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Local YAML rule -> Lakeflow expectation. severity error ≙ expect_or_drop
# (with a quarantine view), severity warn ≙ expect (recorded, kept).
TXN_EXPECTATIONS = {
    "txn_id_not_null": "txn_id IS NOT NULL",
    "amount_positive": "amount > 0",
    "currency_allowed": "currency IN ('BOB','USD','EUR')",
}


@dlt.table(name="silver_transactions", comment="Typed, deduplicated transaction ledger")
@dlt.expect_all_or_drop(TXN_EXPECTATIONS)
@dlt.expect("late_arrivals", "is_late = false")  # warn-severity: recorded, not dropped
def silver_transactions():
    fx = dlt.read("silver_fx_rates")
    return (
        dlt.read("bronze_transactions")
        # dedupe keep-latest, as in the local _latest() helper
        .withColumn(
            "_rn",
            F.row_number().over(
                Window.partitionBy("txn_id").orderBy(F.col("_ingested_at").desc())
            ),
        )
        .where("_rn = 1")
        .select(
            "txn_id",
            "account_id",
            F.to_timestamp("ts").alias("ts"),
            F.col("amount").cast("double").alias("amount"),
            "currency",
            "txn_type",
            "channel",
            "counterparty",
            "merchant_category",
            (F.lower("is_flagged") == "true").alias("is_flagged"),
            F.to_date(F.to_timestamp("ts")).alias("event_date"),
        )
        .join(fx, ["event_date", "currency"], "left")
        .withColumn("amount_bob", F.round(F.col("amount") * F.col("rate_to_bob"), 2))
        .drop("rate_to_bob")
    )


# Quarantine pattern: the rows expectations would drop, kept with their reason —
# the same contract as the local quarantine.<table> with _reject_reason.
@dlt.table(name="quarantine_transactions")
def quarantine_transactions():
    return (
        dlt.read("bronze_transactions")
        .where("txn_id IS NULL OR amount <= 0 OR currency NOT IN ('BOB','USD','EUR')")
        .withColumn("_quarantined_at", F.current_timestamp())
    )
