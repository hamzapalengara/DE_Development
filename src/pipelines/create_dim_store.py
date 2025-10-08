"""
Local PySpark job to create store dimension table.
Creates a dimension table with unique store IDs and surrogate keys.
"""
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id

def main():
    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("CreateStoreDimension") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

    try:
        # Define paths
        project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        silver_sales_path = os.path.join(project_root, "data", "silver", "sales")
        dim_store_path = os.path.join(project_root, "data", "silver", "dim_store")

        print(f"Reading sales data from: {silver_sales_path}")
        
        # Read from Silver sales data
        sales_df = spark.read.parquet(silver_sales_path)
        
        print("Sales schema:")
        sales_df.printSchema()
        print(f"Total sales records: {sales_df.count()}")
        
        # Create store dimension
        print("\nCreating store dimension...")
        dim_store = sales_df.select("store_id").distinct() \
            .withColumn("store_key", monotonically_increasing_id()) \
            .select("store_key", "store_id")

        print("\nStore dimension schema:")
        dim_store.printSchema()
        print(f"Total stores: {dim_store.count()}")
        print("\nSample store data:")
        dim_store.show(5, truncate=False)

        # Write dimension table
        print(f"\nWriting store dimension to: {dim_store_path}")
        dim_store.write.mode("overwrite").parquet(dim_store_path)

        print("\nSuccessfully created store dimension table.")

    except Exception as e:
        print(f"Error creating store dimension: {str(e)}")
        raise

    finally:
        spark.stop()

if __name__ == "__main__":
    main()