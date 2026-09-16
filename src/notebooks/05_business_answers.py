# Databricks notebook source
# MAGIC %md
# MAGIC # 05 Business answers and performance evidence
# MAGIC Run after the Lakeflow Gold pipeline succeeds.

# COMMAND ----------
from pyspark.sql import functions as F

# COMMAND ----------
CATALOG = "workspace"
SCHEMA = "taxi_tanvir"
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## BQ1 Where and when are taxis busiest
# COMMAND ----------
top_zones = spark.sql("""
SELECT pickup_zone, SUM(trips) AS trips
FROM gold_zone_hour
GROUP BY pickup_zone
ORDER BY trips DESC
LIMIT 10
""")
display(top_zones)
top_three = [r.pickup_zone for r in top_zones.limit(3).collect()]
display(spark.table("gold_zone_hour").filter(F.col("pickup_zone").isin(top_three)).groupBy("pickup_zone", "pickup_hour").agg(F.sum("trips").alias("trips")).orderBy("pickup_zone", "pickup_hour"))

# COMMAND ----------
# MAGIC %md
# MAGIC Dispatcher interpretation: The top-ten table identifies the small set of zones that drive most pickup demand. The hourly curve for the top three separates commuter peaks from airport-style demand, so vehicle positioning should follow each zone's shape rather than one city-wide schedule.

# COMMAND ----------
# MAGIC %md
# MAGIC ## BQ2 JFK versus LaGuardia pickups
# COMMAND ----------
display(spark.sql("""
SELECT pickup_zone,
       COUNT(*) AS trips,
       AVG(fare_amount) AS avg_fare,
       100 * AVG(CASE WHEN payment_type = 1 AND fare_amount > 0 THEN tip_amount / fare_amount END) AS card_tip_pct
FROM gold_trips_enriched
WHERE pu_location_id IN (132, 138)
GROUP BY pickup_zone
ORDER BY trips DESC
"""))
# COMMAND ----------
# MAGIC %md
# MAGIC Airport volume and average fare show the operational difference between JFK and LaGuardia. Tip percentage is restricted to card payments because the TLC data does not record cash tips; mixing cash trips into the denominator would understate tipping.

# COMMAND ----------
# MAGIC %md
# MAGIC ## BQ3 Payment mix and value by borough
# COMMAND ----------
display(spark.sql("""
WITH x AS (
  SELECT pickup_borough, payment_label, SUM(trips) AS trips,
         SUM(total_revenue) / SUM(trips) AS avg_total
  FROM gold_payment_daily
  GROUP BY pickup_borough, payment_label
)
SELECT *, trips / SUM(trips) OVER (PARTITION BY pickup_borough) AS trip_share
FROM x ORDER BY pickup_borough, trips DESC
"""))
# COMMAND ----------
# MAGIC %md
# MAGIC The trip-share view shows whether a borough relies more heavily on cash or card. Average totals must be read with care because recorded card totals include tips while cash tips are absent from the source.

# COMMAND ----------
# MAGIC %md
# MAGIC ## BQ4 Yellow versus green
# COMMAND ----------
display(spark.sql("""
SELECT service_type, pickup_borough, COUNT(*) AS trips,
       percentile_approx(trip_distance, 0.5) AS median_distance,
       percentile_approx(duration_min, 0.5) AS median_duration_min,
       AVG(total_amount) AS avg_revenue_per_trip
FROM gold_trips_enriched
GROUP BY service_type, pickup_borough
ORDER BY service_type, trips DESC
"""))
# COMMAND ----------
# MAGIC %md
# MAGIC Yellow and green taxis are compared by geography, median distance, median duration and average revenue. Medians are used for trip shape because a small number of extreme distances or durations would distort an average.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Tuning before and after
# COMMAND ----------
import time

def timed_shuffle(partitions):
    spark.conf.set("spark.sql.shuffle.partitions", partitions)
    started = time.perf_counter()
    rows = spark.table("silver_trips").groupBy("pickup_month", "service_type").agg(F.sum("total_amount")).collect()
    return partitions, round(time.perf_counter() - started, 3), len(rows)

before = timed_shuffle(200)
after = timed_shuffle(16)
timing = spark.createDataFrame([("before", *before), ("after", *after)], "phase string, shuffle_partitions int, elapsed_seconds double, output_rows int")
timing.write.mode("overwrite").saveAsTable("performance_observations")
display(timing)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Idempotency assertion
# COMMAND ----------
key = ["service_type", "vendor_id", "pickup_ts", "dropoff_ts", "pu_location_id", "do_location_id", "total_amount"]
silver = spark.table("silver_trips")
assert silver.count() == silver.select(key).distinct().count(), "Duplicate business keys detected"
print("idempotency_check=passed")
