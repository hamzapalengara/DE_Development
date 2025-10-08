"""
Pipeline to move inventory data from bronze to silver layer.
Applies data standardization and transformations.
"""
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, trim, upper

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("InventoryBronzeToSilver") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

# Define paths
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
bronze_path = os.path.join(project_root, "data", "bronze", "inventory")
silver_path = os.path.join(project_root, "data", "silver", "inventory")

print(f"Reading from bronze layer: {bronze_path}")

# Read from Bronze
inventory_df = spark.read.parquet(bronze_path)

print("Bronze schema:")
inventory_df.printSchema()
print(f"Bronze record count: {inventory_df.count()}")

# Show sample data to understand structure
print("\nSample bronze data:")
inventory_df.show(5, truncate=False)

# Standardize columns / types
# Check if columns exist and apply transformations
if "inventory_qty" in inventory_df.columns:
    inventory_silver = inventory_df \
        .withColumn("inventory_date", to_date(col("inventory_date"), "dd-MM-yyyy")) \
        .withColumn("product_name", trim(col("product_name"))) \
        .withColumn("store_id", trim(col("store_id"))) \
        .withColumn("qty_on_hand", col("inventory_qty").cast("integer")) \
        .select("inventory_date", "product_name", "store_id", "qty_on_hand")
else:
    # If column names are different, adapt accordingly
    print("\nAvailable columns:", inventory_df.columns)
    inventory_silver = inventory_df

print("\nSilver schema:")
inventory_silver.printSchema()
print(f"Silver record count: {inventory_silver.count()}")

# Show sample silver data
print("\nSample silver data:")
inventory_silver.show(5, truncate=False)

# Write to Silver
print(f"\nWriting to silver layer: {silver_path}")
inventory_silver.write.mode("overwrite") \
    .partitionBy("inventory_date") \
    .parquet(silver_path)

print("Successfully processed data to silver layer.")

# Stop Spark
spark.stop()
