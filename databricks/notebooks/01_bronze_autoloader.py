# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze ingestion via Auto Loader
# MAGIC Databricks equivalent of local `pipeline/bronze.py`: verbatim strings + lineage
# MAGIC columns, schema drift rescued instead of raised.

# COMMAND ----------
from pyspark.sql import functions as F

CATALOG, LANDING = "lakekeeper", "/Volumes/lakekeeper/landing"

SOURCES = {
    "customers": "csv",
    "accounts": "csv",
    "transactions": "json",
    "fx_rates": "json",
}

# COMMAND ----------
for table, fmt in SOURCES.items():
    (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", fmt)
        .option("cloudFiles.schemaLocation", f"{LANDING}/_schemas/{table}")
        # Local equivalent: bronze stores strings verbatim; drifted columns raise
        # SchemaDriftError for the ingestion agent. Here: drift lands in _rescued_data.
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("cloudFiles.inferColumnTypes", "false")  # everything as string, like local bronze
        .load(f"{LANDING}/{table}_*")
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_name"))
        .writeStream.option("checkpointLocation", f"{LANDING}/_checkpoints/{table}")
        .trigger(availableNow=True)
        .toTable(f"{CATALOG}.bronze.{table}")
    )
