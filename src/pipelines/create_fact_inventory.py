"""
Local PySpark job to create fact_inventory table by joining inventory data with dimension tables.
Mimics the AWS Glue job but works with local files.
"""
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

def main():
    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("CreateFactInventory") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

    # Define paths
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    
    # Input paths
    inventory_path = os.path.join(project_root, "data", "silver", "inventory")
    dim_product_path = os.path.join(project_root, "data", "silver", "dim_product")
    dim_store_path = os.path.join(project_root, "data", "silver", "dim_store")
    
    # Output path
    fact_inventory_path = os.path.join(project_root, "data", "silver", "fact_inventory")

    print(f"Reading inventory data from: {inventory_path}")
    print(f"Reading dim_product from: {dim_product_path}")
    print(f"Reading dim_store from: {dim_store_path}")

    try:
        # Read inventory data and apply transformations
        inventory_df = spark.read.parquet(inventory_path) \
            .withColumn("inventory_date", to_date(col("inventory_date"))) \
            .withColumnRenamed("qty_on_hand", "qty_on_hand")  # Explicit rename for clarity

        dim_product = spark.read.parquet(dim_product_path)
        dim_store = spark.read.parquet(dim_store_path)

        # Show schema and counts for debugging
        print("\nInventory schema:")
        inventory_df.printSchema()
        print(f"Inventory record count: {inventory_df.count()}")
        
        print("\nProduct dimension schema:")
        dim_product.printSchema()
        print(f"Product dimension record count: {dim_product.count()}")
        
        print("\nStore dimension schema:")
        dim_store.printSchema()
        print(f"Store dimension record count: {dim_store.count()}")

        # Join with dimensions
        print("\nJoining inventory with dimensions...")
        fact_inventory = inventory_df.join(dim_product, "product_name") \
                                   .join(dim_store, "store_id") \
                                   .select(
                                       "inventory_date",
                                       "product_id",
                                       "store_key",
                                       "qty_on_hand"
                                   )

        # Show joined result schema and sample data
        print("\nFact Inventory schema:")
        fact_inventory.printSchema()
        print(f"Fact Inventory record count: {fact_inventory.count()}")
        print("\nSample Fact Inventory data:")
        fact_inventory.show(5, truncate=False)

        # Write to silver/fact_inventory
        print(f"\nWriting fact_inventory to: {fact_inventory_path}")
        fact_inventory.write.mode("overwrite") \
            .partitionBy("inventory_date") \
            .parquet(fact_inventory_path)

        print("\nSuccessfully created fact_inventory table!")

    except Exception as e:
        print(f"Error processing fact_inventory: {str(e)}")
        raise e
    finally:
        # Stop Spark
        spark.stop()

if __name__ == "__main__":
    main()
