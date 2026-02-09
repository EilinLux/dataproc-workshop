# Chapter 03 — Packaging & Submitting Modular PySpark Jobs to Dataproc

## Overview

In this chapter, we move from interactive Jupyter notebooks to modular, production-ready **PySpark** applications submitted via `gcloud dataproc jobs submit pyspark`.

### What We Are Going to Do
1. **Modularize Helper Logic ([`spark_apps/utils.py`](./spark_apps/utils.py))**:
   * Isolate reusable DataFrame transformations (such as `some_utility_function`, which stamps a `processed_timestamp` audit column) into a dedicated helper module.
   * Distribute `utils.py` across all Spark executors using `--py-files`.
2. **Build the Main PySpark Driver ([`spark_apps/process_data.py`](./spark_apps/process_data.py))**:
   * Reads raw CSV sales data (`gs://<bucket>/raw_data/sales/sales_data.csv`) with an explicit `StructType` schema.
   * Filters rows for `--processing-date`, aggregates `SUM(amount) AS total_sales` by `product_id`, applies `some_utility_function`, and writes the result to **BigQuery** (`filtered_sales`).
3. **Upload & Submit the Job ([`scripts/01_package_and_submit_job.sh`](./scripts/01_package_and_submit_job.sh))**:
   * Uploads `sales_data.csv`, `utils.py`, and `process_data.py` to Cloud Storage and submits the job to your running Dataproc cluster.

> **Connection to Part 3 ([`CiCdAirFlowDagTemplate`](https://github.com/EilinLux/CiCdAirFlowDagTemplate))**: In production CI/CD pipelines, helper modules like `utils.py` are packaged into `utils.zip` and automatically synced to Cloud Composer alongside your DAGs!

---

## Chapter Contents

| File | Description |
| :--- | :--- |
| [`03_submit_pyspark_jobs.ipynb`](./03_submit_pyspark_jobs.ipynb) | Interactive guide explaining `gcloud dataproc jobs submit pyspark`, `--py-files`, and BigQuery connector options. |
| [`spark_apps/process_data.py`](./spark_apps/process_data.py) | Main PySpark driver script reading CSV data from GCS and writing aggregated results to BigQuery. |
| [`spark_apps/utils.py`](./spark_apps/utils.py) | Helper Python module distributed to Spark executors via `--py-files`. |
| [`data/sales_data.csv`](./data/sales_data.csv) | Sample retail sales CSV dataset. |
| [`scripts/01_package_and_submit_job.sh`](./scripts/01_package_and_submit_job.sh) | Uploads scripts & data to GCS and submits the PySpark job to your Dataproc cluster. |

---

## How to Run the Script

```bash
bash 03_submit_pyspark_jobs/scripts/01_package_and_submit_job.sh
```
