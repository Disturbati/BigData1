#!/usr/bin/env python3
"""reviewed_productsSQL.py"""

import argparse
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import time
from datetime import datetime
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, split, explode, count, from_unixtime, year, desc, length

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
    StructField("time", IntegerType(), True),
    StructField("summary", StringType(), True),
    StructField("text", StringType(), True)
])

# calculate time elapsed
start_time = time.time()

# read from csv file
input_df = spark.read.option("quote", "\"") \
         .csv(input_path, header=True, schema=schema) \
         .cache()

# convert unix_timestamp to year
input_df = input_df.withColumn("time", input_df["time"].cast("timestamp"))
input_df = input_df.withColumn("year", year(from_unixtime(input_df["time"].cast("bigint")))).cache()

# use sql to calculate the top 10 reviewed products for each year
# and append for each product the top 5 words used in the text field
# of the reviews

# calculate the top 10 reviews for each product in each year
df_with_reviews_number = input_df.groupBy("year", "productId").agg(count("*").alias("review_count")).cache()

window = Window.partitionBy("year").orderBy(desc("review_count"))

df_with_top = df_with_reviews_number.withColumn("row_number", row_number().over(window))

df_with_top = df_with_top.filter(df_with_top["row_number"] <= 10).drop("row_number").cache()

# calculate the top 5 words for each product in each year
# split the text field into words
df_with_words = input_df.withColumn("words", explode(split(input_df["text"], " "))).cache()

df_with_words = df_with_words.filter(length(df_with_words["words"]) > 3).groupBy("year", "productId", "words").agg(count("*").alias("word_count")).cache()

window = Window.partitionBy("year", "productId").orderBy(desc("word_count"))

df_with_top_words = df_with_words.withColumn("row_number", row_number().over(window))
df_with_top_words = df_with_top_words.filter(df_with_top_words["row_number"] <= 5).drop("row_number").cache()

# join the two dataframes
final_df = df_with_top.join(df_with_top_words, ["year", "productId"]).cache()

end_time = time.time()

# Print the time elapsed
print("Time elapsed: ", end_time - start_time)

# save the output
final_df.write.csv(output_path)