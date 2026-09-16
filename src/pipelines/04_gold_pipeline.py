from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = "workspace"
SCHEMA = "taxi_tanvir"


@dp.materialized_view(name="dim_zone", comment="NYC taxi zone dimension")
def dim_zone():
    return spark.table(f"{CATALOG}.{SCHEMA}.bronze_zone_lookup").select(
        F.col("LocationID").cast("int").alias("zone_id"),
        F.col("Zone").alias("zone_name"),
        F.col("Borough").alias("borough"),
        F.col("service_zone"),
    )


@dp.materialized_view(name="gold_trips_enriched", comment="Validated trip facts with readable pickup geography")
@dp.expect_or_drop("valid_pickup_zone", "pu_location_id IS NOT NULL")
@dp.expect_or_drop("non_negative_total", "total_amount >= 0")
def gold_trips_enriched():
    trips = spark.table(f"{CATALOG}.{SCHEMA}.silver_trips").alias("t")
    zones = spark.table(f"{CATALOG}.{SCHEMA}.bronze_zone_lookup").alias("z")
    return trips.join(zones, F.col("t.pu_location_id") == F.col("z.LocationID").cast("int"), "left").select(
        "t.*",
        F.coalesce(F.col("z.Zone"), F.lit("Unknown")).alias("pickup_zone"),
        F.coalesce(F.col("z.Borough"), F.lit("Unknown")).alias("pickup_borough"),
        F.when(F.col("payment_type") == 1, "Credit card")
         .when(F.col("payment_type") == 2, "Cash")
         .when(F.col("payment_type") == 3, "No charge")
         .when(F.col("payment_type") == 4, "Dispute")
         .when(F.col("payment_type") == 5, "Unknown")
         .when(F.col("payment_type") == 6, "Voided")
         .when(F.col("payment_type") == 0, "Flex fare")
         .otherwise("Other").alias("payment_label"),
    )


@dp.materialized_view(name="gold_zone_hour", comment="Pickup zone, date, hour and service metrics")
def gold_zone_hour():
    return spark.read.table("LIVE.gold_trips_enriched").groupBy(
        "pu_location_id", "pickup_zone", "pickup_borough", "pickup_date", "pickup_hour", "service_type"
    ).agg(
        F.count("*").alias("trips"),
        F.sum("total_amount").alias("total_revenue"),
        F.avg("fare_amount").alias("avg_fare"),
        F.avg("trip_distance").alias("avg_distance"),
        F.avg("duration_min").alias("avg_duration_min"),
    )


@dp.materialized_view(name="gold_payment_daily", comment="Daily payment metrics by pickup borough")
def gold_payment_daily():
    return spark.read.table("LIVE.gold_trips_enriched").groupBy(
        "pickup_borough", "pickup_date", "payment_type", "payment_label"
    ).agg(
        F.count("*").alias("trips"),
        F.sum("total_amount").alias("total_revenue"),
        F.avg("total_amount").alias("avg_total"),
        F.avg(F.when(F.col("payment_type") == 1, F.col("tip_amount") / F.when(F.col("fare_amount") > 0, F.col("fare_amount")))).alias("card_tip_rate"),
    )
