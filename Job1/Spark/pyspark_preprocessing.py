from pyspark.sql import SparkSession
import argparse
from pyspark.sql.functions import col, regexp_replace
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import string
import re

# Parsing the arguments
parser = argparse.ArgumentParser()
parser.add_argument("--input", type=str)
parser.add_argument("--output", type=str)
args = parser.parse_args()
input_path = args.input
output_file_cleaned_path = args.output

# Extracting the filename without extension
input_filename = input_path.split("/")[-1].split(".")[0]

# Output file path for cleaned dataset (default value se non è stato passato come parametro --output)
if output_file_cleaned_path is None:
    output_file_cleaned_path = "hdfs:///user/davidemolitierno/input/" + input_filename + '_job1_cleaned'

# Create a Spark session
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

# Reading the input CSV file
csv_df = spark.read.option("quote", "\"").csv(input_path, header=True, schema=schema).cache()

# Skip the header of the CSV file
header = csv_df.first()

# Convert the "Id" column to a numeric type for proper sorting
df_without_header = csv_df.withColumn("Id", col("Id").cast("int"))

# Remove HTML tags and punctuation from the "Text" column
punctuation_pattern = "[!\"#$%&'()*+,-./:;<=>?@\\[\\\\\\]^_`{|}~<>]"
df_cleaned = df_without_header.withColumn("Text", regexp_replace(col("Text"), f"<.*?>|{punctuation_pattern}", ""))

# Sort the DataFrame by the "Id" column in ascending order
sorted_df = df_cleaned.orderBy("Id")

# Recreate the DataFrame with the header and sorted records
output_rdd = spark.createDataFrame([header]).union(sorted_df)

# Write the sorted DataFrame to a CSV file on HDFS
output_rdd.coalesce(1).write.csv(output_file_cleaned_path, header=True, mode="overwrite")