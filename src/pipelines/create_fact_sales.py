"""
Local PySpark job to create fact_sales table by joining sales data with dimension tables.
Mimics the AWS Glue job but works with local files.
"""
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

def main():
    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("CreateFactSales") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

    # Define paths
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    
    # Input paths
    sales_path = os.path.join(project_root, "data", "silver", "sales")
    dim_product_path = os.path.join(project_root, "data", "silver", "dim_product")
    dim_store_path = os.path.join(project_root, "data", "silver", "dim_store")
    
    # Output path
    fact_sales_path = os.path.join(project_root, "data", "silver", "fact_sales")

    print(f"Reading sales data from: {sales_path}")
    print(f"Reading dim_product from: {dim_product_path}")
    print(f"Reading dim_store from: {dim_store_path}")

    try:
        # Read silver-clean tables + dims
        sales_df = spark.read.parquet(sales_path) \
            .withColumn("sale_date", to_date(col("sale_date"))) \
            .withColumnRenamed("qty_sold", "qty_sold")  # Explicit rename for clarity

        dim_product = spark.read.parquet(dim_product_path)
        dim_store = spark.read.parquet(dim_store_path)

        # Show schema and counts for debugging
        print("\nSales schema:")
        sales_df.printSchema()
        print(f"Sales record count: {sales_df.count()}")
        
        print("\nProduct dimension schema:")
        dim_product.printSchema()
        print(f"Product dimension record count: {dim_product.count()}")
        
        print("\nStore dimension schema:")
        dim_store.printSchema()
        print(f"Store dimension record count: {dim_store.count()}")

        # Join with dimensions
        print("\nJoining sales with dimensions...")
        fact_sales = sales_df.join(dim_product, "product_name") \
                            .join(dim_store, "store_id") \
                            .select(
                                "sale_date",
                                "product_id",
                                "store_key",
                                "qty_sold"
                            )

        # Show joined result schema and sample data
        print("\nFact Sales schema:")
        fact_sales.printSchema()
        print(f"Fact Sales record count: {fact_sales.count()}")
        print("\nSample Fact Sales data:")
        fact_sales.show(5, truncate=False)

        # Write to silver/fact_sales
        print(f"\nWriting fact_sales to: {fact_sales_path}")
        fact_sales.write.mode("overwrite") \
            .partitionBy("sale_date") \
            .parquet(fact_sales_path)

        print("\nSuccessfully created fact_sales table!")

    except Exception as e:
        print(f"Error processing fact_sales: {str(e)}")
        raise e
    finally:
        # Stop Spark
        spark.stop()

if __name__ == "__main__":
    main()
