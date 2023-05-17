from pyspark.sql import SparkSession
import argparse
from datetime import datetime
import re
import string

# parsing degli argomenti
parser = argparse.ArgumentParser()
parser.add_argument("--input", type=str)

args = parser.parse_args()
input_path = args.input
# get name last file from input_path removing extension
input_filename = input_path.split("/")[-1].split(".")[0]

# il dataset cleanato ha lo stesso nome + '_job1_cleaned'
output_file_cleaned_path = "hdfs:///user/davidegattini/input/" + input_filename + '_job1_cleaned'

# spark session
spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

# funzione che esegue la pulizia del testo
#   1. eliminati tag html
#   2. eliminata punteggiatura
#   3. eliminati caratteri di spaziatura
def cleanText(text_review):
    # pulito da punteggiatura e tag html
    html_regex = re.compile(r"<.*?>") # regex per togliere tag html
    text_review = html_regex.sub("", text_review) # esegue sostituzione

    # rimuovi la punteggiatura
    text_review = text_review.translate(str.maketrans(string.punctuation, ' '*len(string.punctuation)))

    # rimuovi tutti i caratteri di spaziatura
    text_review = " ".join(text_review.split())
    return text_review

# reading input
csv_rdd = sc.textFile(input_path)
header = csv_rdd.first() # header del file usato per skipparlo nella fase di preparazione dei dati
header_rdd = sc.parallelize([header]) # crea rdd con solo l'header del csv

# Preprocessing
#   1. skip dell'header del file csv
#   2. splitting dei campi 
#   3. return di un RDD cui record sono fatti dalla selezione del productId, year (ottenuto convertendo lo unixtimestamp) e il testo della recensione pulito
input_rdd = csv_rdd.filter(lambda x: x != header).map(lambda x: x.strip().split(",")).map(lambda x: (x[0] + "," + x[1] + "," + x[2] + "," +  x[3] + "," +  x[4] + "," +  x[5] + "," +  x[6] + "," +  x[7] + "," +  x[8] + "," +  cleanText(x[9])))

output_rdd = header_rdd.union(input_rdd.coalesce(1)).repartition(2) # unisci l'header del csv con il contenuto del csv

output_rdd.sortByKey().coalesce(1).saveAsTextFile(output_file_cleaned_path)