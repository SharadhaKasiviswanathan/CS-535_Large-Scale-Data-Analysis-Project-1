#!/usr/bin/env python
# coding: utf-8

# In[15]:


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

# In[16]:


dfa=spark.read.parquet("s3://bsu-c535-fall2024-commons/arjun-workspace/articles/")
dfa.show()


# In[17]:


dfl=spark.read.parquet("s3://bsu-c535-fall2024-commons/arjun-workspace/linktarget/")
dfl.show()


# In[18]:


dfp=spark.read.parquet("s3://bsu-c535-fall2024-commons/arjun-workspace/page/")
dfp.show()


# In[19]:


dfpl=spark.read.parquet("s3://bsu-c535-fall2024-commons/arjun-workspace/pagelinks/")
dfpl.show()


# In[6]:


dfr=spark.read.parquet("s3://bsu-c535-fall2024-commons/arjun-workspace/redirect/")
dfr.show()


# In[20]:


# Step 1: Handling redirects
# Map page redirects to their final destinations
redirects = dfr.select("rd_from", "rd_title").alias("redirects")

# Join with the page dataset to resolve redirects
page_with_redirects = dfp.join(redirects, dfp.page_title == redirects.rd_title, "left_outer") \
                         .selectExpr("page_id", "coalesce(rd_title, page_title) as resolved_page_title")

page_with_redirects.show()


# In[21]:


# Step 2: Joining pagelinks and page
page_links = dfpl.alias("pl").join(
    dfp.alias("p"), dfpl.pl_target_id == dfp.page_id
).select("pl.pl_from", "p.page_id").alias("page_links")

page_links.show()


# In[22]:


# Step 3: Finding symmetric pairs (links in both directions)

# Aliasing the pagelinks DataFrame to avoid ambiguity
dfpl_a = dfpl.alias("a")
dfpl_b = dfpl.alias("b")

# Joining the pagelinks DataFrame on matching targets
symmetric_pairs = dfpl_a.join(
    dfpl_b, 
    (dfpl_a["pl_from"] == dfpl_b["pl_target_id"]) & 
    (dfpl_a["pl_target_id"] == dfpl_b["pl_from"])
)

# Selecting the necessary columns
symmetric_pairs = symmetric_pairs.select(
    dfpl_a["pl_from"].alias("page_a"), 
    dfpl_a["pl_target_id"].alias("page_b")
)

# Removing duplicate pairs and reversing pairs  
final_pairs = symmetric_pairs.filter("page_a < page_b")

final_pairs.show()


# In[24]:


final_pairs.count()


# In[12]:


# Step 4: Writing results to the output path in parquet format
import os

output_path = "s3://sharadhakasi/page_pairs_output/"

# Writing the DataFrame to Parquet format, overwriting if it already exists
final_pairs.write.mode("overwrite").parquet(output_path)


# In[25]:


# Get the original count
original_count = final_pairs.count()

# Get the count after dropping duplicates
unique_pairs = final_pairs.dropDuplicates()
unique_count = unique_pairs.count()

# Compare the two counts
if original_count == unique_count:
    print("No duplicates found.")
else:
    print(f"Found {original_count - unique_count} duplicate pairs.")


# In[ ]:




