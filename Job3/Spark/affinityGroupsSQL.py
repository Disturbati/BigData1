#!/usr/bin/env python3
"""affinityGroupsSQL.py"""

import argparse
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
from pyspark.sql.functions import col, collect_set, size, explode, array, udf
import itertools
from pyspark.storagelevel import StorageLevel

# Parse the arguments
parser = argparse.ArgumentParser()
parser.add_argument("--input", help="the input path")
parser.add_argument("--output", help="the output path")

args = parser.parse_args()

input_path, output_path = args.input, args.output

# Create the SparkSession
spark = SparkSession.builder \
    .config("spark.executor.memory", "4g") \
    .config("spark.executor.cores", "4") \
    .getOrCreate()

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
            .persist(StorageLevel(False, True, False, False, 1))
            

# filter the input file with reviews that have a score of 4 or 5
filtered_df = input_df.select("userId", "productId", "score") \
    .filter(input_df["score"] >= 4)

# join the filtered_df with itself on the productId where the userId is different
joined_df = input_df.alias("df1").join(input_df.alias("df2"), ["productId"]) \
    .where(col("df1.userId") < col("df2.userId")) \
    .select(col("df1.userId").alias("userId1"), col("df2.userId").alias("userId2"), col("df1.productId").alias("productId")) 
    # .distinct() \

# get the couple of users that have affinity for at least 3 products
couple_df = joined_df.groupBy("userId1", "userId2").agg(collect_set("productId").alias("productsAffinity")) \
    .where(size(col("productsAffinity")) >= 3) \
    .select("userId1", "userId2", "productsAffinity") \
    .persist(StorageLevel(False, True, False, False, 1))
    

# udf to get all the possible combinations of the products affinity
@udf(ArrayType(ArrayType(StringType())))
def get_combinations(values):
    list = []
    for i in range(3, len(values) + 1):
        list.extend(itertools.combinations(values, i))
    return list

# get all the possible combinations of the products affinity
couple_df = couple_df.repartition("userId1", "userId2") \
    .withColumn("productsAffinity", explode(get_combinations(col("productsAffinity"))))


    # explode the users in the couple_df
user_df = couple_df.repartition("productsAffinity") \
    .withColumn("userId", explode(array("userId1", "userId2")))

# group the users by the products they have affinity for
group_df = couple_df.groupBy("productsAffinity") \
    .agg(collect_set("userId1").alias("usersAffinity")) \
    .select("productsAffinity", "usersAffinity") \

# write the top 10 affinity groups to the console
group_df.show(10, False)

# write the otuput to the output path
group_df.write.json(output_path, mode="overwrite")