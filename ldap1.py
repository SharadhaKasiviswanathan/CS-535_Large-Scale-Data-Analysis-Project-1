#!/usr/bin/env python
# coding: utf-8

import pyspark
from pyspark.sql import SparkSession # Import SparkSession

# Initialize a Spark session
spark = (
    SparkSession.builder
    #.remote('sc://localhost:15002')
    .appName("My App")
    .getOrCreate()
)


dfa=spark.read.parquet("s3://bsu-c535-fall2024-commons/arjun-workspace/articles/")
dfa.show()

dflt=spark.read.parquet("s3://bsu-c535-fall2024-commons/arjun-workspace/linktarget/")
dflt.show()

dfp=spark.read.parquet("s3://bsu-c535-fall2024-commons/arjun-workspace/page/")
dfp.show()

dfpl=spark.read.parquet("s3://bsu-c535-fall2024-commons/arjun-workspace/pagelinks/")
dfpl.show()

dfr=spark.read.parquet("s3://bsu-c535-fall2024-commons/arjun-workspace/redirect/")
dfr.show()


from pyspark.sql import (
    SparkSession,
    functions as F,
    types as T
)

import pyspark
from pyspark import StorageLevel
from datetime import datetime


# Registering DataFrames as temporary views
dfp.createOrReplaceTempView("page")
dfr.createOrReplaceTempView("redirect")
dfpl.createOrReplaceTempView("pagelinks")
dflt.createOrReplaceTempView("linktarget")


spark.table("page") \
    .join(
        spark.table("redirect"),
        F.expr("page_namespace = rd_namespace and page_title = rd_title"),
        "inner"
    ) \
    .select(
        F.col("rd_from").alias("resolved_source_id"),
        F.col("page_id").alias("resolved_target_id"),
    ) \
    .distinct() \
    .persist(StorageLevel.MEMORY_AND_DISK) \
    .createOrReplaceTempView("page_redirect")

page_redirect_df = spark.table("page_redirect")
page_redirect_df.printSchema()
page_redirect_df.show(20, truncate=False)

page_redirect_df.count()


spark.table("linktarget") \
    .join(
        spark.table("page"),
        F.expr("lt_title = page_title and page_namespace = lt_namespace"),
        "inner"
    ) \
    .select(
        F.col("page_id"),
        F.col("lt_id").alias("linktarget_id"),
        F.col("page_namespace")
    ) \
    .persist(StorageLevel.MEMORY_AND_DISK) \
    .createOrReplaceTempView("linktarget_page")

linktargetpage_df = spark.table("linktarget_page")
linktargetpage_df.printSchema()
linktargetpage_df.show(20, truncate=False)

linktargetpage_df.count()

linktargetpage_df.createOrReplaceTempView("linktarget_page")


spark.table("linktarget_page") \
    .join(
        spark.table("pagelinks"),
        F.expr("linktarget_id = pl_target_id"), #page_namespace = pl_from_namespace
        "inner"
    ) \
    .select(
        F.col("pl_from").alias("source_id"),  # Assuming pl_from column which we want from pagelink
        F.col("page_id").alias("target_id")
    ) \
    .distinct() \
    .persist(StorageLevel.MEMORY_AND_DISK) \
    .createOrReplaceTempView("linktargetpage_pagelink")

linktargetpage_pagelink_df = spark.table("linktargetpage_pagelink")
linktargetpage_pagelink_df.printSchema()
linktargetpage_pagelink_df.show(20, truncate=False)

linktargetpage_pagelink_df.count()

linktargetpage_pagelink_df.createOrReplaceTempView("linktargetpage_pagelink")


spark.table("page_redirect") \
    .join(
        spark.table("linktargetpage_pagelink"),
        F.expr("resolved_source_id = source_id"),
        "left"
    ) \
    .select(
        F.col("source_id").alias("page_a"),              # source_id from linktargetpage_pagelink as page_a
        F.col("resolved_target_id").alias("page_b")      # resolved target_id from page_redirect as page_b
    ) \
    .distinct() \
    .persist(StorageLevel.MEMORY_AND_DISK) \
    .createOrReplaceTempView("final_page_links")
final_page_links_df = spark.table("final_page_links")
final_page_links_df.printSchema()
final_page_links_df.show(20, truncate=False)

final_page_links_df.createOrReplaceTempView("final_page_links")


# Self-joining to get only mutual links
mutual_links_df = spark.table("final_page_links").alias("df1") \
    .join(
        spark.table("final_page_links").alias("df2"),
        (F.col("df1.page_a") == F.col("df2.page_b")) & (F.col("df1.page_b") == F.col("df2.page_a")),
        "left"
    ) \
    .select(
        F.when(F.col("df1.page_a") < F.col("df1.page_b"), F.col("df1.page_a")).otherwise(F.col("df1.page_b")).alias("page_a"),
        F.when(F.col("df1.page_a") < F.col("df1.page_b"), F.col("df1.page_b")).otherwise(F.col("df1.page_a")).alias("page_b")
    ) \
    .distinct() \
    .persist(StorageLevel.MEMORY_AND_DISK)

# Creating a view for the final mutual links without repeats
mutual_links_df.createOrReplaceTempView("unique_final_page_links")
mutual_links_df.printSchema()
mutual_links_df.show(20, truncate=False)

mutual_links_df.count()


#Writing results to the output path in parquet format
import os
output_path = "s3://sharadhakasi/ldap1-results/PAGE_PAIRS_OUTPUT/"
# Writing the DataFrame to Parquet format, overwriting if it already exists
mutual_links_df.write.mode("overwrite").parquet(output_path)


