"""
Pipeline to move inventory data from landing zone to bronze layer.

This script reads inventory data from the landing zone, adds processing metadata,
and writes it to the bronze layer with partitioning.
"""
import os
import sys
from datetime import datetime
from functools import reduce
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit, to_date, current_date

# Project paths
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
LANDING_PATH = os.path.join(PROJECT_ROOT, "data", "landing", "inventory")
BRONZE_PATH = os.path.join(PROJECT_ROOT, "data", "bronze", "inventory")

def load_landing_data(spark, landing_path):
    """Load data from landing zone, including all subdirectories."""
    # Initialize an empty list to store DataFrames
    dfs = []
    
    # Walk through all subdirectories
    for root, _, files in os.walk(landing_path):
        # Filter for supported file types
        data_files = [
            os.path.join(root, f) 
            for f in files 
            if f.lower().endswith(('.csv', '.parquet', '.json'))
        ]
        
        for file_path in data_files:
            try:
                print(f"Loading file: {file_path}")
                # Determine file type and read accordingly
                if file_path.endswith('.csv'):
                    df = spark.read.option("header", "true").csv(file_path)
                elif file_path.endswith('.parquet'):
                    df = spark.read.parquet(file_path)
                elif file_path.endswith('.json'):
                    df = spark.read.json(file_path)
                else:
                    continue
                
                # Add source file path for tracking
                df = df.withColumn("source_file", lit(file_path))
                dfs.append(df)
                
            except Exception as e:
                print(f"Error processing {file_path}: {str(e)}")
    
    if not dfs:
        print(f"No valid data files found in {landing_path} or its subdirectories")
        return None
    
    # Combine all DataFrames if there are multiple
    if len(dfs) > 1:
        return reduce(DataFrame.unionAll, dfs)
    return dfs[0]

def process_to_bronze():
    """Process data from landing to bronze layer."""
    # Initialize Spark
    spark = SparkSession.builder \
        .appName("InventoryToBronze") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()
        
    # Define paths
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    landing_path = os.path.join(project_root, "data", "landing", "inventory")
    bronze_path = os.path.join(project_root, "data", "bronze", "inventory")
    
    try:
        # Ensure output directory exists
        os.makedirs(bronze_path, exist_ok=True)
        
        # Read data from landing zone
        print(f"Reading data from: {landing_path}")
        df = load_landing_data(spark, landing_path)
        
        if df is None:
            print("No data to process.")
            return
            
        # Add metadata
        df = df.withColumn("processing_timestamp", lit(datetime.now()))
        df = df.withColumn("load_date", to_date(current_date()))
        
        # Show schema and count for verification
        print("Schema:")
        df.printSchema()
        print(f"Number of records: {df.count()}")
        
        # Write to bronze layer with partitioning
        print(f"Writing to bronze layer: {bronze_path}")
        df.write.mode("overwrite") \
            .partitionBy("load_date") \
            .parquet(bronze_path)
            
        print("Successfully processed data to bronze layer.")
        print(f"Data written to: {bronze_path}")
        
    except Exception as e:
        print(f"Error processing data: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    process_to_bronze()
