#!/usr/bin/env python3
"""affinityGroupsSQL.py"""

import argparse
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


# Parse the arguments
parser = argparse.ArgumentParser()
parser.add_argument("--input", help="the input path")
parser.add_argument("--output", help="the output path")

args = parser.parse_args()

input_path, output_path = args.input, args.output

# Create the SparkSession
spark = SparkSession.builder.getOrCreate()

# Define the schema structure
schema = StructType([
    StructField("Id", IntegerType(), True),
    StructField("productId", StringType(), True),
    StructField("userId", StringType(), True),
    StructField("profileName", StringType(), True),
    StructField("helpfulnessNumerator", IntegerType(), True),
    StructField("helpfulnessDenominator", IntegerType(), True),
    StructField("score", StringType(), True),
    StructField("time", StringType(), True),
    StructField("summary", StringType(), True),
    StructField("text", StringType(), True)
])

# Use sql to read the input file
input_df = spark.read.option("quote", "\"") \
            .csv(input_path, header=False, schema=schema) \
            .cache()

# filter the input file with reviews that have a score of 4 or 5
filtered_df = input_df.select("userId", "productId", "score") \
    .filter("score" >= 4).cache()

# join the filtered_df with itself on the productId where the userId is different
joined_df = filtered_df.join(filtered_df, "productId") \
    .filter("userId != userId1") \
    .select("userId", "userId1", "productID") \
    .distinct() \
    
# group the joined_df by userId and userId1 and count the number of times they appear together and save
# the set of products they have the same affinity for
grouped_df = joined_df.groupBy("userId", "userId1").agg({"productID": "count"}) \
    .withColumnRenamed("count(productID)", "affinity") \
    .groupBy("userId", "affinity").agg({"userId1": "collect_set"}) \
    .withColumnRenamed("collect_set(userId1)", "affinityGroup") \
    .orderBy("affinity", ascending=False)
