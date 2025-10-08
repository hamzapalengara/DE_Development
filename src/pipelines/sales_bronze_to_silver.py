"""
Local PySpark job to move sales data from bronze to silver layer.
Mimics the AWS Glue job but works with local files.
"""
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

def main():
    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("SalesBronzeToSilver") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

    try:
        # Define paths
        project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        bronze_path = os.path.join(project_root, "data", "bronze", "sales")
        silver_path = os.path.join(project_root, "data", "silver", "sales")

        print(f"Reading from bronze layer: {bronze_path}")
        
        # Read from Bronze
        sales_df = spark.read.parquet(bronze_path)
        
        print("Bronze schema:")
        sales_df.printSchema()
        print(f"Record count: {sales_df.count()}")
        print("\nSample data:")
        sales_df.show(5, truncate=False)

        # Standardize columns / types
        sales_silver = sales_df \
            .withColumn("sale_date", to_date(col("sale_date"), "dd-MM-yyyy")) \
            .withColumnRenamed("quantity_sold", "qty_sold") \
            .select("sale_date", "product_name", "store_id", "qty_sold")

        print("\nSilver schema:")
        sales_silver.printSchema()
        print(f"Record count: {sales_silver.count()}")
        print("\nSample silver data:")
        sales_silver.show(5, truncate=False)

        # Write to Silver
        print(f"\nWriting to silver layer: {silver_path}")
        sales_silver.write.mode("overwrite") \
            .partitionBy("sale_date") \
            .parquet(silver_path)

        print("\nSuccessfully processed sales data to silver layer.")

    except Exception as e:
        print(f"Error processing data: {str(e)}")
        raise

    finally:
        spark.stop()

if __name__ == "__main__":
    main()