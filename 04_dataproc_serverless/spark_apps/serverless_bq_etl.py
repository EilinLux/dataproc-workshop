"""
Chapter 04 Reference PySpark Batch Job: Dataproc Serverless BigQuery ETL
========================================================================
Reference Notebook: ../04_dataproc_serverless.ipynb
Submission Script:  ../scripts/01_submit_serverless_batch.sh
Next Workshop:      Part 2 (`composer-workshop/04_dataproc_serverless_etl`)

Purpose:
  Executes a serverless BigQuery-to-BigQuery aggregation using Dataproc Serverless
  for Spark without provisioning or managing any cluster VMs.
"""

from __future__ import annotations

import argparse
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType


def parse_args() -> argparse.Namespace:
    """Parses command-line arguments passed to the Dataproc Serverless batch job."""
    parser = argparse.ArgumentParser(
        description="Aggregate daily retail sales from BigQuery using Dataproc Serverless."
    )
    parser.add_argument(
        "--input-table",
        type=str,
        required=True,
        help="Source BigQuery table in 'project.dataset.table' format.",
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
        help="Target transaction date to process (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--gcs-temp-bucket",
        type=str,
        required=True,
        help="Cloud Storage bucket used for Spark BigQuery connector temporary staging.",
    )
    return parser.parse_args()


def main() -> None:
    """Main entry point for the Dataproc Serverless PySpark batch application."""
    args = parse_args()

    # Dataproc Serverless Runtime 1.1+ / 2.2+ bundles the Spark BigQuery connector
    # out of the box—no external Maven coordinates (`spark.jars.packages`) needed.
    spark = (
        SparkSession.builder.appName("SalesDataProcessor-Serverless-BQ")
        .config("temporaryGcsBucket", args.gcs_temp_bucket)
        .getOrCreate()
    )

    try:
        # 1. Read source BigQuery table via the Storage Read API
        print(f"Reading source data from BigQuery table: {args.input_table}")
        df = spark.read.format("bigquery").option("table", args.input_table).load()

        # 2. Filter by processing_date, compute total_sales per product_id, and
        #    append `processed_timestamp` (matching the `filtered_sales` schema
        #    created in Chapter 03 where `processed_timestamp` is REQUIRED).
        print(f"Filtering and aggregating transactions for date: {args.processing_date}")
        processed_df = (
            df.filter(
                F.col("transaction_date") == F.lit(args.processing_date).cast(DateType())
            )
            .groupBy("product_id")
            .agg(F.sum("amount").alias("total_sales"))
            .withColumn("processing_date", F.lit(args.processing_date).cast(DateType()))
            .withColumn("processed_timestamp", F.current_timestamp())
        )

        # 3. Write aggregated DataFrame back to BigQuery
        print(f"Writing aggregated results to BigQuery table: {args.output_table}")
        (
            processed_df.write.format("bigquery")
            .option("table", args.output_table)
            .option("temporaryGcsBucket", args.gcs_temp_bucket)
            .mode("overwrite")
            .save()
        )
        print("Dataproc Serverless PySpark job completed successfully!")

    except Exception as exc:
        print(f"Error during Dataproc Serverless execution: {exc}", file=sys.stderr)
        raise

    finally:
        # Always close SparkSession cleanly in `finally`
        if "spark" in locals() and spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()
