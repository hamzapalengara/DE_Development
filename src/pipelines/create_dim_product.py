"""
Local PySpark job to create product dimension table.
Creates a dimension table with unique product names and surrogate keys.
"""
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id

def main():
    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("CreateProductDimension") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

    try:
        # Define paths
        project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        silver_sales_path = os.path.join(project_root, "data", "silver", "sales")
        dim_product_path = os.path.join(project_root, "data", "silver", "dim_product")

        print(f"Reading sales data from: {silver_sales_path}")
        
        # Read from Silver sales data
        sales_df = spark.read.parquet(silver_sales_path)
        
        print("Sales schema:")
        sales_df.printSchema()
        print(f"Total sales records: {sales_df.count()}")
        
        # Create product dimension
        print("\nCreating product dimension...")
        dim_product = sales_df.select("product_name").distinct() \
            .withColumn("product_id", monotonically_increasing_id()) \
            .select("product_id", "product_name")

        print("\nProduct dimension schema:")
        dim_product.printSchema()
        print(f"Total products: {dim_product.count()}")
        print("\nSample product data:")
        dim_product.show(5, truncate=False)

        # Write dimension table
        print(f"\nWriting product dimension to: {dim_product_path}")
        dim_product.write.mode("overwrite").parquet(dim_product_path)

        print("\nSuccessfully created product dimension table.")

    except Exception as e:
        print(f"Error creating product dimension: {str(e)}")
        raise

    finally:
        spark.stop()

if __name__ == "__main__":
    main()