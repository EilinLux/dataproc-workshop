"""
Chapter 03 Helper Module: PySpark DataFrame Utilities (utils.py)
================================================================
Reference Notebook: ../03_submit_pyspark_jobs.ipynb
Main Driver Script: ./process_data.py

Purpose:
  Demonstrates how to factor shared PySpark DataFrame transformations into a
  standalone Python module (`utils.py`) that is distributed to all Spark
  worker nodes via `--py-files` (or packaged inside `utils.zip` in Part 3:
  `CiCdAirFlowDagTemplate`).
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def some_utility_function(df: DataFrame) -> DataFrame:
    """
    Appends an audit column `processed_timestamp` containing the current execution
    timestamp to the provided Spark DataFrame.

    Args:
        df: Input PySpark DataFrame after business aggregations.

    Returns:
        DataFrame with an additional `processed_timestamp` (TIMESTAMP) column.
    """
    return df.withColumn("processed_timestamp", F.current_timestamp())
