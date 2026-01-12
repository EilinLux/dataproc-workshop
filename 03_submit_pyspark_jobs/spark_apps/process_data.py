"""
Chapter 03 Reference PySpark Driver: GCS CSV to BigQuery ETL (process_data.py)
==============================================================================
Reference Notebook: ../03_submit_pyspark_jobs.ipynb
Helper Module:      ./utils.py
Submission Script:  ../scripts/01_package_and_submit_job.sh

Execution Flow:
  1. Parses CLI arguments (`--input-path`, `--output-table`, `--processing-date`,
     and `--gcs-temp-bucket`).
  2. Imports `some_utility_function` from `utils.py` (distributed via `--py-files`).
  3. Reads raw CSV transactions from Cloud Storage using an explicit `StructType`
     schema (avoiding an expensive extra pass for `inferSchema=True`).
  4. Filters rows for `--processing-date` and aggregates `SUM(amount) AS total_sales`
     grouped by `product_id`.
  5. Writes the transformed DataFrame to BigQuery (`--output-table`).
"""

from __future__ import annotations

import argparse
import sys

# PySpark SQL imports for session management, column functions, and explicit schema types
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, DoubleType, StringType, StructField, StructType

# ------------------------------------------------------------------------------
# 1. Import Helper Module (`utils.py` distributed via `--py-files`)
# ------------------------------------------------------------------------------
# When Dataproc executes `gcloud dataproc jobs submit pyspark --py-files=gs://.../utils.py`,
# it places `utils.py` on the top-level PYTHONPATH of both driver and executors.
try:
    from utils import some_utility_function
except ImportError:
    try:
        from .utils import some_utility_function
    except ImportError:
        print(
            "Warning: Could not import utils.py. Ensure it is passed via --py-files.",
            file=sys.stderr,
        )
        some_utility_function = lambda df: df  # noqa: E731


def parse_args() -> argparse.Namespace:
    """Parses command-line arguments passed to the PySpark job."""
    parser = argparse.ArgumentParser(
        description="Process raw sales CSV data from GCS and load aggregations into BigQuery."
    )
    parser.add_argument(
        "--input-path",
        type=str,
        required=True,
        help="Cloud Storage URI to raw sales CSV data (e.g., gs://my-bucket/raw_data/sales/).",
    )
    parser.add_argument(
        "--output-table",
        type=str,
        required=True,
        help="Destination BigQuery table in 'project.dataset.table' format.",
    )
    parser.add_argument(
        "--processing-date",
        type=str,
        required=True,
        help="Target transaction date to filter on (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--gcs-temp-bucket",
        type=str,
        default="example-dataproc-workshop",
        help="Cloud Storage bucket used by the Spark BigQuery connector for temporary staging.",
    )
    return parser.parse_args()


def main() -> None:
    """Main entry point for the Dataproc cluster PySpark job."""
    args = parse_args()

    # --------------------------------------------------------------------------
    # 2. Initialize SparkSession
    # --------------------------------------------------------------------------
    spark = (
        SparkSession.builder.appName("SalesDataProcessor-Cluster")
        .config("temporaryGcsBucket", args.gcs_temp_bucket)
        .getOrCreate()
    )

    # --------------------------------------------------------------------------
    # 3. Define Explicit Input CSV Schema
    # --------------------------------------------------------------------------
    # Providing an explicit StructType avoids scanning the entire CSV file twice
    # and guarantees strict type enforcement (`DoubleType`, `DateType`).
    input_schema = StructType(
        [
            StructField("transaction_id", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("amount", DoubleType(), True),
            StructField("transaction_date", DateType(), True),
        ]
    )

    try:
        # ----------------------------------------------------------------------
        # 4. Extract: Read Raw CSV from Cloud Storage
        # ----------------------------------------------------------------------
        print(f"Reading raw CSV sales data from: {args.input_path}")
        df = (
            spark.read.option("header", "true")
            .schema(input_schema)
            .csv(args.input_path)
        )

        # ----------------------------------------------------------------------
        # 5. Transform: Filter by Date, Aggregate by Product & Apply Utility
        # ----------------------------------------------------------------------
        print(f"Filtering and aggregating data for date: {args.processing_date}")
        processed_df = (
            df.filter(
                F.col("transaction_date") == F.lit(args.processing_date).cast(DateType())
            )
            .groupBy("product_id")
            .agg(F.sum("amount").alias("total_sales"))
            .withColumn("processing_date", F.lit(args.processing_date).cast(DateType()))
        )

        # Apply the helper function from `utils.py`
        processed_df = some_utility_function(processed_df)

        # ----------------------------------------------------------------------
        # 6. Load: Write Aggregated Table to BigQuery
        # ----------------------------------------------------------------------
        print(f"Writing processed data to BigQuery table: {args.output_table}")
        (
            processed_df.write.format("bigquery")
            .option("table", args.output_table)
            .option("temporaryGcsBucket", args.gcs_temp_bucket)
            .mode("overwrite")
            .save()
        )
        print("PySpark job completed successfully!")

    except Exception as exc:
        print(f"An error occurred during PySpark job execution: {exc}", file=sys.stderr)
        raise

    finally:
        # Always stop the SparkSession cleanly in `finally`
        if "spark" in locals() and spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()
