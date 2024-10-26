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

# Creating a Packaging Script
@echo off

REM Name of the zip file
set ZIP_FILE=project_package.zip

REM Remove any existing zip file with the same name
del %ZIP_FILE%

REM Add LSDA_P1.py and requirements.txt to the zip file
powershell Compress-Archive -Path LSDA_P1.py,requirements.txt -DestinationPath %ZIP_FILE%
echo Created %ZIP_FILE with LSDA_P1.py as the entry point and dependencies.

