"""
Pipeline to move inventory data from landing zone to bronze layer.
Simple PySpark job similar to AWS Glue structure.
"""
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, to_date

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("InventoryToBronze") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

# Define paths
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
landing_path = os.path.join(project_root, "data", "landing", "inventory")
bronze_path = os.path.join(project_root, "data", "bronze", "inventory")

# Read from landing zone (all CSV files in subdirectories)
inventory_df = spark.read \
    .option("header", "true") \
    .csv(f"{landing_path}/*/*.csv")

# Add load_date column
inventory_df = inventory_df.withColumn("load_date", to_date(current_date()))

# Show schema and count for verification
print("Schema:")
inventory_df.printSchema()
print(f"Number of records: {inventory_df.count()}")

# Write to Bronze as Parquet partitioned by load_date
print(f"Writing to bronze layer: {bronze_path}")
inventory_df.write.mode("overwrite") \
    .partitionBy("load_date") \
    .parquet(bronze_path)

print("Successfully processed data to bronze layer.")

# Stop Spark
spark.stop()
