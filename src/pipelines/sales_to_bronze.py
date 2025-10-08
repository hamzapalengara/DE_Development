"""
Pipeline to move sales data from landing zone to bronze layer.
Simple PySpark job similar to AWS Glue structure.
"""
import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, to_date

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("SalesToBronze") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

# Define paths
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
landing_path = os.path.join(project_root, "data", "landing", "sales")
bronze_path = os.path.join(project_root, "data", "bronze", "sales")

logger.info(f"Starting sales data processing job")
logger.info(f"Reading data from {landing_path}")

# Read from landing zone (all CSV files in subdirectories)
sales_df = spark.read \
    .option("header", "true") \
    .csv(f"{landing_path}/*/*.csv")

# Log basic info about the data
row_count = sales_df.count()
logger.info(f"Read {row_count} rows from sales feed")

if row_count > 0:
    logger.info("Sample data schema:")
    sales_df.printSchema()
    logger.info("First row of data:")
    sales_df.show(1, vertical=True)
else:
    logger.warning("No data found in the source")
    spark.stop()
    exit(0)

# Add load_date column
sales_df = sales_df.withColumn("load_date", to_date(current_date()))

# Write to Bronze as Parquet partitioned by load_date
logger.info(f"Writing data to {bronze_path}")
sales_df.write.mode("overwrite") \
    .partitionBy("load_date") \
    .parquet(bronze_path)

logger.info(f"Successfully wrote {row_count} rows to {bronze_path}")
logger.info("Job completed successfully")

# Stop Spark
spark.stop()
