# Evidence index

This directory contains the same final execution evidence used in the local delivery package and Jira submission.

## Visual evidence

1. `full_dag_graph.svg` — the deployed five-stage dependency graph: Land → Bronze → Silver → Gold → Analysis.
2. `successful_run_summary.svg` — the successful acceptance run summary, including duration, query count and row I/O.

Both visuals are displayed directly in the repository root README and are also attached individually to P1880-781.

## Verifiable records

- `RUN_RESULTS.md` — row counts, data-quality results, analytical answers, performance comparison and successful job-run details.
- `DATABRICKS_RESOURCES.md` — direct workspace links and resource identifiers.
- `docs/JIRA_SUBMISSION.md` — the final Jira description, ordered comments, attachment manifest and completion status.

The visuals provide a quick review surface; the recorded identifiers, source, workflow definition and runbook make the result reproducible.
