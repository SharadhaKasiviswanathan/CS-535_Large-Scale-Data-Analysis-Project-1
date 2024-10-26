# CS-535_Large-Scale-Data-Analysis-Project-1

import os
from pyspark.sql import SparkSession

# Initialize Spark Session
spark = (
    SparkSession.builder
    .appName("Mutual Links Finder")
    .master("local[*]")  # Update this for your specific Spark setup
    .getOrCreate()
)

# Retrieve the S3 output path from the environment variable
output_path = os.getenv("PAGE_PAIRS_OUTPUT", "s3://default-output-path/")

spark.stop()
