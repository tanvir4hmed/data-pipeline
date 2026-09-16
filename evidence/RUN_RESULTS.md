# P1880-781 execution evidence

Execution date: 2026-09-15 (Asia/Dhaka)  
Workspace: Databricks trial, serverless compute  
Catalog/schema: `workspace.taxi_tanvir`

## Source landing

Seven public NYC TLC files were landed in a Unity Catalog Volume: yellow and green trip Parquet files for October-December 2024, plus the taxi zone lookup CSV.

## Bronze

Auto Loader completed with schema tracking and checkpointing. A second run completed without duplicating rows.

| Table | Rows |
|---|---:|
| `bronze_yellow` | 11,148,511 |
| `bronze_green` | 162,363 |
| `bronze_zone_lookup` | 265 |

## Silver

- Type conformance, common yellow/green schema and derived date/time fields completed.
- Four null-safe validation rules were evaluated.
- Four duplicate records were removed.
- Mandatory-null and uniqueness assertions passed.

## Gold Lakeflow Declarative Pipeline

Pipeline ID: `2d0d93cb-a503-4cb6-90b4-b6dc992d1ab2`

| Materialized view | Output | Result |
|---|---:|---|
| `dim_zone` | 265 | Completed |
| `gold_trips_enriched` | approximately 11M | Completed; 2 expectations met |
| `gold_zone_hour` | approximately 319K | Completed |
| `gold_payment_daily` | approximately 3.3K | Completed |

The validate-only run and full refresh both completed.

## Selected analytical results

- Midtown Center peaked at 18:00 with 42,349 trips in the three-month sample.
- JFK: 480,149 pickups, average fare 64.94, recorded card tip rate 21.13%.
- LaGuardia: 328,712 pickups, average fare 45.15, recorded card tip rate 26.09%.
- Manhattan payment mix was led by credit card (7,469,561 trips; 76.77%).
- Yellow Manhattan trips: 9,633,930; median distance 1.59; median duration 12.77 minutes.
- Green Manhattan trips: 96,014; median distance 1.81; median duration 11.68 minutes.

## Tuning and repeatability

| Phase | Shuffle partitions | Elapsed seconds | Result rows |
|---|---:|---:|---:|
| Before | 200 | 0.666 | 14 |
| After | 16 | 0.614 | 14 |

The tuned run was 7.8% faster for this representative aggregation. Auto Loader checkpointing, overwrite-based curated outputs and uniqueness assertions make re-runs repeatable without duplicate accumulation.

## Lakeflow Job

- Job name: `P1880-781 NYC Taxi Lakehouse`
- Job ID: `993328277314420`
- Successful run ID: `980778669583774`
- Result: Succeeded on 2026-09-15 in 1 minute 46 seconds
- `01_land_sources`: Succeeded in 27 seconds
- `02_bronze`: Succeeded in 34 seconds
- `03_silver`: Succeeded in 42 seconds
- Queries: 24
- Rows read: 78,537,220
- Rows written: 10,991,418
- Trigger: active file-arrival trigger on the raw Unity Catalog Volume

The successful run followed an intentional rerun after the landing validation was hardened to check the seven expected filenames. This prevents Auto Loader metadata entries from being mistaken for duplicate source files.
