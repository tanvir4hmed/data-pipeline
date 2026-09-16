# Databricks notebook source
# MAGIC %md
# MAGIC # 01 Setup and land
# MAGIC Creates the Unity Catalog schema and volume, then downloads the seven public NYC TLC files only when absent.

# COMMAND ----------
import os
import shutil
import urllib.request

CATALOG = "workspace"
SCHEMA = "taxi_tanvir"
VOLUME = "raw"
BASE = "https://d37ci6vzurychx.cloudfront.net"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")
VOL = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"

wanted = [f"trip-data/yellow_tripdata_2024-{m:02d}.parquet" for m in (10, 11, 12)]
wanted += [f"trip-data/green_tripdata_2024-{m:02d}.parquet" for m in (10, 11, 12)]
wanted += ["misc/taxi_zone_lookup.csv"]

for rel in wanted:
    name = rel.rsplit("/", 1)[-1]
    destination = f"{VOL}/{name}"
    if os.path.exists(destination):
        print(f"skip existing: {name}")
        continue
    print(f"download: {name}")
    # Serverless compute blocks driver-local /tmp access through dbutils.
    # Stream directly into the Unity Catalog Volume instead.
    with urllib.request.urlopen(f"{BASE}/{rel}") as response, open(destination, "wb") as output:
        shutil.copyfileobj(response, output, length=8 * 1024 * 1024)

files = dbutils.fs.ls(VOL)
landed_names = {item.name.rstrip("/") for item in files}
expected_names = {rel.rsplit("/", 1)[-1] for rel in wanted}
missing = expected_names - landed_names
assert not missing, f"Missing landed files: {sorted(missing)}"
display([item for item in files if item.name.rstrip("/") in expected_names])
