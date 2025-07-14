# utils.py
from pyspark.sql import functions as F

def some_utility_function(df):
    """
    Example utility function to add a timestamp column.
    """
    return df.withColumn("processed_timestamp", F.current_timestamp())