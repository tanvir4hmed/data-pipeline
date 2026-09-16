# Databricks notebook source
# MAGIC %md
# MAGIC # 02 Bronze Auto Loader
# MAGIC Ingests yellow and green parquet files incrementally. Checkpoints and schema locations make repeated runs idempotent.

# COMMAND ----------
from pyspark.sql import functions as F

CATALOG = "workspace"
SCHEMA = "taxi_tanvir"
VOL = f"/Volumes/{CATALOG}/{SCHEMA}/raw"
STATE = f"/Volumes/{CATALOG}/{SCHEMA}/raw/_state"


def ingest_parquet(pattern, table_name, state_name):
    stream = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", f"{STATE}/schema/{state_name}")
        .load(f"{VOL}/{pattern}")
        .withColumn("_source_file", F.col("_metadata.file_name"))
        .withColumn("_ingest_ts", F.current_timestamp())
    )
    query = (
        stream.writeStream.option("checkpointLocation", f"{STATE}/checkpoints/{state_name}")
        .trigger(availableNow=True)
        .toTable(f"{CATALOG}.{SCHEMA}.{table_name}")
    )
    query.awaitTermination()


ingest_parquet("yellow_tripdata_2024-*.parquet", "bronze_yellow", "yellow")
ingest_parquet("green_tripdata_2024-*.parquet", "bronze_green", "green")

zones = (
    spark.read.option("header", True).option("inferSchema", True)
    .csv(f"{VOL}/taxi_zone_lookup.csv")
    .withColumn("_source_file", F.col("_metadata.file_path"))
    .withColumn("_ingest_ts", F.current_timestamp())
)
zones.write.format("delta").mode("overwrite").option("overwriteSchema", True).saveAsTable(
    f"{CATALOG}.{SCHEMA}.bronze_zone_lookup"
)

counts = [(name, spark.table(f"{CATALOG}.{SCHEMA}.{name}").count()) for name in
          ("bronze_yellow", "bronze_green", "bronze_zone_lookup")]
display(spark.createDataFrame(counts, "table_name string, row_count long"))
