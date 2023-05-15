from pyspark.sql import SparkSession
import argparse

# parsing degli argomenti
parser = argparse.ArgumentParser()
parser.add_argument("--input", type=str)
parser.add_argument("--output", type=str)

args = parser.parse_args()
input_path = args.input
output_folder_path = args.output

# spark session
spark = SparkSession.builder.getOrCreate()

# reading input
csv_rdd = spark.textFile(input_path)
# skip header 
header = csv_rdd.first()
# file di input senza header
input_rdd = csv_rdd.filter(lambda x: x != header).map(lambda x: x.strip().split(","))


# processamento (trasformazioni, azioni, persistenza)

