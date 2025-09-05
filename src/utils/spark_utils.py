from pyspark.sql import SparkSession
from typing import Optional, Dict

def create_spark(app_name: str, configs: Optional[Dict[str, str]] = None) -> SparkSession:
    builder = SparkSession.builder.appName(app_name)
    if configs:
        for k, v in configs.items():
            builder = builder.config(k, v)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark