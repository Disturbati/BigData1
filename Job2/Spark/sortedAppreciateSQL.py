#!/usr/bin/env python3
"""sortedAppreciateSQL.py"""

import argparse
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import time

# Parse the arguments
parser = argparse.ArgumentParser()
parser.add_argument("--input", help="the input path")

args = parser.parse_args()

input_path = args.input

# Create the SparkSession
spark = SparkSession.builder.appName("sortedAppreciateSQL").getOrCreate()

# Use sql to read the input file
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

# calculate time elapsed
start_time = time.time()
# ================================
input_df = spark.read.csv(input_path, header=False, schema=schema).cache()

# Use sql to filter the input file
filtered_df = input_df.select("userId", "helpfulnessNumerator", "helpfulnessDenominator").filter("helpfulnessNumerator >= 0 and helpfulnessDenominator > 0 and helpfulnessNumerator <= helpfulnessDenominator") \
    .withColumn("appreciate", input_df["helpfulnessNumerator"] / input_df["helpfulnessDenominator"]) \
    .select("userId", "appreciate").groupBy("userId").avg("appreciate").orderBy("avg(appreciate)", ascending=False)
# ================================
end_time = time.time()

# Print the time elapsed
print("Time elapsed: ", end_time - start_time)

# Write the first 10 rows of the output file
filtered_df.show(10)