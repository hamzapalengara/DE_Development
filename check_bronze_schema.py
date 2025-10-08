"""Quick script to check bronze data schema"""
import os
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("CheckSchema") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

project_root = os.path.dirname(os.path.abspath(__file__))
bronze_path = os.path.join(project_root, "data", "bronze", "inventory")

print(f"Reading from: {bronze_path}")
df = spark.read.parquet(bronze_path)

print("\nSchema:")
df.printSchema()

print(f"\nRecord count: {df.count()}")

print("\nColumn names:")
print(df.columns)

print("\nSample data:")
df.show(5, truncate=False)

spark.stop()
