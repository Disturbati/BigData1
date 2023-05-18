#!/usr/bin/env python3
"""affinityGroupsSQL.py"""

import argparse
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, collect_set, size, explode, array
from pyspark.sql.window import Window

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
    .filter(input_df["score"] >= 4)

# join the filtered_df with itself on the productId where the userId is different
joined_df = input_df.alias("df1").join(input_df.alias("df2"), ["productId"]) \
    .where(col("df1.userId") < col("df2.userId")) \
    .select(col("df1.userId").alias("userId1"), col("df2.userId").alias("userId2"), col("df1.productId").alias("productId")) \
    .distinct()

# get the couple of users that have affinity for at least 3 products
couple_df = joined_df.groupBy("userId1", "userId2").agg(collect_set("productId").alias("productsAffinity")) \
    .where(size(col("productsAffinity")) >= 3) \
    .select("userId1", "userId2", "productsAffinity") \
    .cache()
    
# group the users by the products they have affinity for
group_df = couple_df.withColumn(
    "userId",
    explode(array("userId1", "userId2"))
).groupBy("productsAffinity").agg(collect_set("userId").alias("groupUsers"))

# write the top 10 affinity groups to the console
group_df.show(10, False)

# write the otuput to the output path
group_df.write.json(output_path, mode="overwrite")