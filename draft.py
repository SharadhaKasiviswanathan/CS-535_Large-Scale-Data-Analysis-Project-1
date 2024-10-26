#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pyspark
from pyspark.sql import SparkSession # Import SparkSession

# Initialize a Spark session
spark = (
    SparkSession.builder
    .remote('sc://localhost:15002')
    .appName("My App")
    .getOrCreate()
)


# #### Loading the data from S3 into DataFrames

# In[2]:


dfa=spark.read.parquet("s3://bsu-c535-fall2024-commons/arjun-workspace/articles/")
dfa.show()


# In[4]:


dflt=spark.read.parquet("s3://bsu-c535-fall2024-commons/arjun-workspace/linktarget/")
dflt.show()


# In[5]:


dfp=spark.read.parquet("s3://bsu-c535-fall2024-commons/arjun-workspace/page/")
dfp.show()


# In[6]:


dfpl=spark.read.parquet("s3://bsu-c535-fall2024-commons/arjun-workspace/pagelinks/")
dfpl.show()


# In[7]:


dfr=spark.read.parquet("s3://bsu-c535-fall2024-commons/arjun-workspace/redirect/")
dfr.show()


# In[8]:


from pyspark.sql import (
    SparkSession,
    functions as F,
    types as T
)

from pyspark import StorageLevel
import datetime


# In[9]:


# Registering DataFrames as temporary views
dfp.createOrReplaceTempView("page")
dfr.createOrReplaceTempView("redirect")
dfpl.createOrReplaceTempView("pagelinks")


# In[10]:


spark.table("page") \
    .join(
        spark.table("redirect"),
        F.expr("page_id = rd_from"),
        "left"
    ) \
    .select(
        F.col("page_id").alias("original_id"),
        F.col("page_title"),
        F.col("page_namespace"),
        F.col("rd_title").alias("redirect_target")
    ) \
    .persist(StorageLevel.MEMORY_AND_DISK) \
    .createOrReplaceTempView("page_redirect")

page_redirect_df = spark.table("page_redirect")
page_redirect_df.show(20, truncate=False)


# In[11]:


page_redirect_df.count()


# In[12]:


spark.table("pagelinks") \
    .join(
        spark.table("page_redirect"),
        F.expr("pl_target_id = original_id"),
        "left"
    ) \
    .select(
        F.col("pl_from"),
        F.col("pl_from_namespace"),
        F.col("redirect_target").alias("final_target")
    ) \
    .persist(StorageLevel.MEMORY_AND_DISK) \
    .createOrReplaceTempView("pagelink_resolved")

pagelink_resolved_df = spark.table("pagelink_resolved")
pagelink_resolved_df.show(20, truncate=False)


# In[13]:


pagelink_resolved_df.count()


# In[17]:


# Step 3: Self Join pagelink_resolved with page_redirect to find mutual page pairs

spark.table("pagelink_resolved").alias("pr1") \
    .join(
        spark.table("page_redirect").alias("prd"),  # Alias for page_redirect view
        F.expr("pr1.pl_from = prd.original_id AND pr1.final_target = prd.redirect_target"), 
        "inner"
    ) \
    .select(
        F.col("pr1.pl_from").alias("page_a"),        # Use `pr1` alias for pagelink_resolved columns
        F.col("prd.original_id").alias("page_b")      # Use `prd` alias for page_redirect columns as page_b
    ) \
    .filter(
        F.col("page_a").isNotNull() &                # Filter out null values for page_a
        F.col("page_b").isNotNull()                  # Filter out null values for page_b
    ) \
    .persist(StorageLevel.MEMORY_AND_DISK) \
    .createOrReplaceTempView("page_pairs")           

spark.table("page_pairs").printSchema()

spark.table("page_pairs").show(20, truncate=False)

