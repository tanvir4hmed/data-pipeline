# Databricks notebook source
# MAGIC %md
# MAGIC # 03 Silver quality
# MAGIC Conforms yellow and green trips, standardizes types, removes duplicates, reports four quality rules and writes a clean idempotent Silver table.

# COMMAND ----------
from functools import reduce
from pyspark.sql import functions as F

CATALOG = "workspace"
SCHEMA = "taxi_tanvir"
YELLOW = f"{CATALOG}.{SCHEMA}.bronze_yellow"
GREEN = f"{CATALOG}.{SCHEMA}.bronze_green"
SILVER = f"{CATALOG}.{SCHEMA}.silver_trips"

target = [
    "service_type", "vendor_id", "pickup_ts", "dropoff_ts", "passenger_count",
    "trip_distance", "rate_code", "payment_type", "pu_location_id", "do_location_id",
    "fare_amount", "tip_amount", "tolls_amount", "total_amount", "congestion_surcharge",
]


def conform(df, service, pickup_col, dropoff_col):
    return df.select(
        F.lit(service).alias("service_type"),
        F.col("VendorID").cast("int").alias("vendor_id"),
        F.col(pickup_col).cast("timestamp").alias("pickup_ts"),
        F.col(dropoff_col).cast("timestamp").alias("dropoff_ts"),
        F.col("passenger_count").cast("int").alias("passenger_count"),
        F.col("trip_distance").cast("double").alias("trip_distance"),
        F.col("RatecodeID").cast("int").alias("rate_code"),
        F.col("payment_type").cast("int").alias("payment_type"),
        F.col("PULocationID").cast("int").alias("pu_location_id"),
        F.col("DOLocationID").cast("int").alias("do_location_id"),
        F.col("fare_amount").cast("double").alias("fare_amount"),
        F.col("tip_amount").cast("double").alias("tip_amount"),
        F.col("tolls_amount").cast("double").alias("tolls_amount"),
        F.col("total_amount").cast("double").alias("total_amount"),
        F.col("congestion_surcharge").cast("double").alias("congestion_surcharge"),
    )


y = conform(spark.table(YELLOW), "yellow", "tpep_pickup_datetime", "tpep_dropoff_datetime")
g = conform(spark.table(GREEN), "green", "lpep_pickup_datetime", "lpep_dropoff_datetime")
trips = y.unionByName(g)

prepared = (
    trips.withColumn("pickup_date", F.to_date("pickup_ts"))
    .withColumn("pickup_hour", F.hour("pickup_ts"))
    .withColumn("pickup_dow", F.date_format("pickup_ts", "EEEE"))
    .withColumn("pickup_month", F.date_format("pickup_ts", "yyyy-MM"))
    .withColumn("duration_min", (F.col("dropoff_ts").cast("long") - F.col("pickup_ts").cast("long")) / 60)
    .withColumn("is_airport", F.col("pu_location_id").isin(132, 138))
)

dedup_key = ["service_type", "vendor_id", "pickup_ts", "dropoff_ts", "pu_location_id", "do_location_id", "total_amount"]
before_dedup = prepared.count()
deduplicated = prepared.dropDuplicates(dedup_key)
after_dedup = deduplicated.count()
print(f"duplicate_rows_removed={before_dedup - after_dedup}")

checks = {
    "dropoff_after_pickup": F.col("dropoff_ts") > F.col("pickup_ts"),
    "duration_between_1_360": F.col("duration_min").between(1, 360),
    "distance_0_to_200": F.col("trip_distance").between(0, 200),
    "total_amount_non_negative": F.col("total_amount") >= 0,
}

total = after_dedup
summary = []
for name, condition in checks.items():
    failed = deduplicated.filter(~F.coalesce(condition, F.lit(False))).count()
    summary.append((name, total, failed, round(100.0 * failed / total, 4)))
quality_summary = spark.createDataFrame(summary, "rule string, rows_checked long, rows_failed long, pct_failed double")
quality_summary.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.quality_summary")
display(quality_summary)

all_valid = reduce(lambda left, right: left & right, [F.coalesce(c, F.lit(False)) for c in checks.values()])
clean = deduplicated.filter(all_valid)
(
    clean.write.format("delta").mode("overwrite").option("overwriteSchema", True)
    .partitionBy("pickup_month").saveAsTable(SILVER)
)

silver = spark.table(SILVER)
assert silver.filter("pickup_ts IS NULL OR pu_location_id IS NULL OR total_amount IS NULL").count() == 0
assert silver.select(dedup_key).distinct().count() == silver.count()
display(silver.groupBy("service_type").count())
