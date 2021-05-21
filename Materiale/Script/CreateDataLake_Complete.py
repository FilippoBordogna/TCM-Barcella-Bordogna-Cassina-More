###### TEDx-Load-Aggregate-Model
######

import sys
import json
import pyspark
from pyspark.sql.functions import col, collect_list, array_join, struct

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

##### FROM FILES
tedx_dataset_path = "s3://unibg-data-2021-bordogna-filippo/tedx_dataset_complete.csv"
#barce#tedx_dataset_path = "s3://bucket-dati/tedx_dataset_complete.csv"
#cassi#tedx_dataset_path = "s3://unibg-tcm-lab/tedx_dataset.csv" #CAMBIA CON IL NUOVO FILE CIULA

###### READ PARAMETERS
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

##### START JOB CONTEXT AND JOB
sc = SparkContext()

glueContext = GlueContext(sc)
spark = glueContext.spark_session
 
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

#### READ INPUT FILES TO CREATE AN INPUT DATASET
tedx_dataset = spark.read \
    .option("header","true") \
    .option("quote", "\"") \
    .option("escape", "\"") \
    .option("multiline","true").csv(tedx_dataset_path)
    
#tedx_dataset.printSchema()

#### FILTER ITEMS WITH NULL POSTING KEY
#count_items = tedx_dataset.count()
#count_items_null = tedx_dataset.filter("idx is not null").count()

#print(f"Number of items from RAW DATA {count_items}")
#print(f"Number of items from RAW DATA with NOT NULL KEY {count_items_null}")

## READ TAGS DATASET
tags_dataset_path = "s3://unibg-data-2021-bordogna-filippo/tags_dataset.csv"
#barce# tags_dataset_path = "s3://bucket-dati/tags_dataset.csv"
#cassi# tags_dataset_path = "s3://unibg-tcm-lab/tags_dataset.csv"

tags_dataset = spark.read.option("header","true").csv(tags_dataset_path)

# CREATE THE AGGREGATE MODEL, ADD TAGS TO TEDX_DATASET
tags_dataset_agg = tags_dataset.groupBy(col("idx").alias("idx_ref")).agg(collect_list("tag").alias("tags"))
#tags_dataset_agg.printSchema()

# JOIN BETWEEN TALKS AND TAGS
tedx_dataset_agg = tedx_dataset.join(tags_dataset_agg, tedx_dataset.idx == tags_dataset_agg.idx_ref, "left") \
    .drop("idx_ref") \
    .select(col("idx").alias("_id"), col("*")) \
    .drop("idx") \
   
#tedx_dataset_agg.printSchema()

## READ WATCH NEXT DATASET
watch_next_dataset_path = "s3://unibg-data-2021-bordogna-filippo/watch_next_dataset.csv"
#barce# watch_next_dataset_path = "s3://bucket-dati/watch_next_dataset.csv"
#cassi# watch_next_dataset_path = "s3://unibg-tcm-lab/watch_next_dataset.csv"

watch_next_dataset = spark.read.option("header","true").csv(watch_next_dataset_path)

# DROP THE DUPLICATE ROWS
watch_next_dataset=watch_next_dataset.dropDuplicates()

# AGGREGATE THE WATCH-NEXT
watch_next_dataset_agg=watch_next_dataset.groupBy(col("idx").alias("idx_ref_2")).agg(collect_list(struct("url", "watch_next_idx")).alias("watch_next"))
#watch_next_dataset_agg.printSchema()

# JOIN BETWEEN THE DATA AGGREGATE BEFORE AND THE WATCH-NEXT
tedx_dataset_agg_f = tedx_dataset_agg.join(watch_next_dataset_agg, tedx_dataset_agg._id == watch_next_dataset_agg.idx_ref_2, "left") \
    .drop("idx_ref_2")

#tedx_dataset_agg_f.printSchema()

mongo_uri = "mongodb://clustertcm-shard-00-00.nvexe.mongodb.net:27017,clustertcm-shard-00-01.nvexe.mongodb.net:27017,clustertcm-shard-00-02.nvexe.mongodb.net:27017"
#barce# mongo_uri = "mongodb://tcm-shard-00-00.d0u9l.mongodb.net:27017,tcm-shard-00-01.d0u9l.mongodb.net:27017,tcm-shard-00-02.d0u9l.mongodb.net:27017"
#cassi# mongo_uri = "mongodb://mycluster-shard-00-00.r8cy1.mongodb.net:27017,mycluster-shard-00-01.r8cy1.mongodb.net:27017,mycluster-shard-00-02.r8cy1.mongodb.net:27017"

write_mongo_options = {
    "uri": mongo_uri,
    "database": "unibg_tedx_2021",
    "collection": "tedx_data",
    "username": "admin",
    "password": "admin",
    "ssl": "true",
    "ssl.domain_match": "false"}
from awsglue.dynamicframe import DynamicFrame
tedx_dataset_dynamic_frame = DynamicFrame.fromDF(tedx_dataset_agg_f, glueContext, "nested")

glueContext.write_dynamic_frame.from_options(tedx_dataset_dynamic_frame, connection_type="mongodb", connection_options=write_mongo_options)
