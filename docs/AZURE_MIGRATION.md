# Azure Databricks reproduction guide

The code in `deliverables` is intentionally portable. The data model, Auto Loader checkpoints, quality rules, Lakeflow pipeline and business queries stay the same in Azure Databricks. Only workspace provisioning and compute selection change.

## Azure setup

1. Create an Azure Databricks workspace in the nearest region with available quota.
2. Prefer the Trial Premium tier when the subscription offers it; otherwise use Premium within the free credit.
3. For a classic workspace create one single-node cluster named `taxi-tanvir` using Databricks Runtime 15.4 LTS or later, Photon off, the smallest available general-purpose node, and ten-minute auto termination.
4. If the Azure workspace is serverless, attach notebooks to serverless compute and skip classic cluster creation.
5. Confirm that the `workspace` catalog is visible. If not, replace the `CATALOG` constant in every notebook with the available Unity Catalog name.
6. Import the deliverables, create schema `taxi_tanvir`, run the notebooks in order, and create the Lakeflow Declarative Pipeline and Job from the supplied definitions.

## Cost controls

- Stop classic compute whenever work pauses; keep auto termination at ten minutes.
- Use one small node only. The dataset is roughly 200 MB and does not require workers.
- Check Azure Cost Management after each working session and set a small budget alert.
- Do not enable GPU or ML runtimes.

## Evidence checklist

- Workspace overview and compute configuration
- Seven files in the Unity Catalog volume
- Bronze counts and source-file distribution
- Silver quality summary and duplicate count
- Lakeflow expectations and successful pipeline update
- Five-task Job DAG, non-cron trigger and successful repeated run
- Gold result tables or charts for BQ1 through BQ4
- Before and after tuning timings
- Cluster stopped or serverless run completed
