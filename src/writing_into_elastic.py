#!/usr/bin/env python
# coding: utf-8

# In[1]:


# import os

# spark_version = '3.1.2'
# os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:{}'.format(spark_version)


# In[2]:


import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from constants import TOPIC_NAME, KAFKA_HOST

# Create a Spark session.
spark = SparkSession.builder\
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2")\
        .config('spark.driver.bindAddress','10.0.0.16').appName("Clickstream Data Processing").getOrCreate()

# Read the clickstream data from Kafka.
df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", KAFKA_HOST).option("subscribe", TOPIC_NAME).load()

# Aggregate the clickstream data by URL and country.
agg_df = df.groupBy("url", "country").agg(
    count("*").alias("clicks"),
    countDistinct("user_id").alias("unique_users"),
    avg(unix_timestamp("timestamp") - unix_timestamp("click_data.timestamp")).alias("average_time_spent")
)

# Write the aggregated data to Elasticsearch.
agg_df.writeStream.format("org.elasticsearch.spark.sql").mode("append").option("es.nodes", "localhost:9200").option("es.index", "clickstream").start()

# Start the Spark streaming job.
spark.streams.awaitTermination()


# In[ ]:




