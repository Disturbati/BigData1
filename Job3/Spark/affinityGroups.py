#!/usr/bin/env python3
"""affinityGroups.py"""

import argparse
import pyspark
from pyspark.sql import SparkSession
from operator import add
import itertools
import re

# Parse the arguments
parser = argparse.ArgumentParser()
parser.add_argument("--input", help="the input path")
parser.add_argument("--output", help="the output path")

args = parser.parse_args()

input_path, output_path = args.input, args.output

def split(line):
    # Replace commas inside quotes with semicolons
    line = re.sub(r',(?=[^"]*"[^"]*(?:"[^"]*"[^"]*)*$)', lambda m: m.group().replace(',', ';'), line)
    # Split the line using commas as a delimiter
    fields = line.split(',')
    # Replace semicolons back to commas
    line = [field.replace(';', ',') for field in fields]
    return line

# Create the SparkSession
spark = SparkSession.builder.getOrCreate()

# Read the input file and remove the header
input_rdd = spark.sparkContext.textFile(input_path)
header = input_rdd.first()
input_rdd = input_rdd.filter(lambda line: line != header) \
    .cache()

# filter the input file with reviews that have a score of 4 or 5
filtered_rdd = input_rdd.map(lambda line: split(line)) \
    .filter(lambda line: int(line[6]) >= 4) \
    .map(lambda line: (line[1], line[2])) 

# join the filtered_rdd with itself on the productId where the userId is different
joined_rdd = filtered_rdd.join(filtered_rdd) \
    .filter(lambda line: line[1][0] < line[1][1]) \
    .map(lambda line: ((line[1][0], line[1][1]), line[0]))

# get the couple of users that have affinity for at least 3 products
couple_rdd = joined_rdd.groupByKey() \
    .map(lambda line: (line[0][0], line[0][1], set(line[1]))) \
    .filter(lambda line: len(line[2]) >= 3) 

# get all the possible combinations of the products affinity
def get_combinations(values):
    return set(itertools.combinations(values, 3))
    
couple_rdd = couple_rdd.map(lambda line: (line[0], line[1], get_combinations(line[2]))) \
    .flatMap(lambda line: [(line[0], line[1], x) for x in line[2]]) 

# get the affinity groups
affinity_groups_rdd = couple_rdd.map(lambda line: (line[2], [line[0], line[1]])) \
    .reduceByKey(lambda a, b: set(a) | set(b)) \
    .map(lambda line: (line[0], set(line[1]))) \
    .sortBy(lambda line: list(line[1])[0], ascending=False)


# save the output
affinity_groups_rdd.saveAsTextFile(output_path)

