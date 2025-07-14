# process_data.py
import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType

# Import utilities if you have them in utils.py
try:
    from .utils import some_utility_function
except ImportError:
    print("Could not import utils.py. Ensure it's in --py-files.")
    some_utility_function = lambda x: x # Fallback or handle appropriately

def main():
    parser = argparse.ArgumentParser(description="Process sales data with PySpark.")
    parser.add_argument("--input-path", type=str, required=True,
                        help="GCS path to raw sales data (e.g., gs://my-gcp-bucket/raw_data/sales/).")
    parser.add_argument("--output-table", type=str, required=True,
                        help="BigQuery output table (e.g., my_dataset.processed_sales).")
    parser.add_argument("--processing-date", type=str, required=True,
                        help="Date for processing data (YYYY-MM-DD).")

    args = parser.parse_args()

    spark = SparkSession.builder \
        .appName("SalesDataProcessor") \
        .config("temporaryGcsBucket", "eurobet-dataproc-workshop") \
        .getOrCreate()

    # Define schema for input data (adjust as per your actual data)
    input_schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("transaction_date", DateType(), True)
    ])

    try:
        print(f"Reading data from: {args.input_path}")
        df = spark.read \
            .option("header", "true") \
            .schema(input_schema) \
            .csv(args.input_path)

        print(f"Processing data for date: {args.processing_date}")
        # Example processing: Filter by date and calculate total sales
        processed_df = df.filter(F.col("transaction_date") == F.lit(args.processing_date)) \
                         .groupBy("product_id") \
                         .agg(F.sum("amount").alias("total_sales")) \
                         .withColumn("processing_date", F.lit(args.processing_date))

        # Example of using a utility function
        # processed_df = some_utility_function(processed_df)

        print(f"Writing processed data to BigQuery table: {args.output_table}")


        # Write to BigQuery
        processed_df.write \
            .format("bigquery") \
            .option("table", args.output_table) \
            .mode("overwrite").save()
        # mode can be  "append" or "overwrite" or "ignore" or "errorifexists"
        print("PySpark job completed successfully!")

    except Exception as e:
        print(f"An error occurred: {e}")
        spark.stop()
        raise # Re-raise the exception to indicate job failure

    finally:
        spark.stop()

if __name__ == "__main__":
    main()