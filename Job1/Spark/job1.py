from pyspark.sql import SparkSession
import argparse
from datetime import datetime
import re
import string

# funzione che esegue la pulizia del testo
#   1. eliminati tag html
#   2. eliminata punteggiatura
#   3. eliminati caratteri di spaziatura
#   4. tutto in lower case
def cleanText(text_review):
    # pulito da punteggiatura e tag html
    html_regex = re.compile(r"<.*?>") # regex per togliere tag html
    text_review = html_regex.sub("", text_review) # esegue sostituzione

    # rimuovi la punteggiatura
    text_review = text_review.translate(str.maketrans(string.punctuation, ' '*len(string.punctuation)))

    # rimuovi tutti i caratteri di spaziatura
    text_review = " ".join(text_review.split())
    return text_review.lower()


# TODO: prendi lo start time per calcolare quanto ci mette
# TODO: aggiungere .cache() ove necessario

def preprocessing(line):
    # Replace commas inside quotes with semicolons
    line = re.sub(r',(?=[^"]*"[^"]*(?:"[^"]*"[^"]*)*$)', lambda m: m.group().replace(',', ';'), line)
    # Split the line using commas as a delimiter
    fields = line.split(',')
    # Replace semicolons back to commas
    fields = [field.replace(';', ',') for field in fields]
    return ((datetime.fromtimestamp(int(fields[7])).year, fields[1]), cleanText(fields[9]))

def top_reviewed_products_with_frequent_word_used(input_path, output_folder_path):
    # spark session
    with SparkSession.builder.getOrCreate() as spark:
        sc = spark.sparkContext
        # reading input
        csv_rdd = sc.textFile(input_path)

        # Preprocessing
        #   1. Evita che , negli username creino problemi
        #   2. skip dell'header del file csv
        #   3. splitting dei campi 
        #   4. return di un RDD cui record sono fatti dalla selezione del year (ottenuto convertendo lo unixtimestamp), productId e il testo della recensione pulito
        header = csv_rdd.first() # header del file usato per skipparlo nella fase di preparazione dei dati
        csv_rdd = csv_rdd.filter(lambda x: x != header)
        input_rdd = csv_rdd.map(preprocessing)

        # adesso abbiamo input_rdd:
        # (year, productId), recensione_txt

        # count delle recensioni e append dei testi
        counting_list = list(input_rdd.countByKey().items()) # [(year,productId), count]
        counting_rdd = sc.parallelize(counting_list) # necessario riportarlo in formato rdd

        # TODO: controllare se è necessario fare il join
        year_productId2count_txtRevs = input_rdd.join(counting_rdd) # (year,productId), (txt_rev, count)

        # ora gli elementi dell'rdd sono così strutturati
        # chiave: (year,productId)
        # valore: (txt_revw,count)
        # NB. ho elementi duplicati per un dato prodotto in un certo anno ed un certo contatore 
        #     quindi, da più elementi con i vari testi delle review
        #     devo generarne uno solo cui testo è l'unione (append) di tutti i testi

        # preso un prodotto in un dato anno, eseguo la riduzione che permette di fare quanto detto sopra:
        #   chiave: (year,productId)
        #   valore: (txt_all_revws,count)
        y_pId2count_allRevs = year_productId2count_txtRevs.reduceByKey(lambda x,y: (x[0]+" "+y[0],x[1]))

        # dovrei avere questo per ordinare tutti i prodotti per un certo anno in base al numero delle recensioni
        # year, (productId, count, txt_all_revs)
        y2pId_count_txtAllRevs = y_pId2count_allRevs.map(lambda x: (x[0][0],(x[0][1], x[1][1], x[1][0])))

        # per ogni anno, ordina i prodotti in base al numero di recensioni
        # 1. eseguo il groupBy sulla chiave "year"
        # 2. per ogni valore associato (il valore è una lista di elementi, ogni elemento è (productId, count, txt_all_revs))
        #   2.1. eseguo il sorted della lista, prendendo per ogni suo elemento il secondo campo (count) 
        #   2.2. di questa lista ordianta prendo solo i primi 10
        y2pId_count_txtAllRevs_orderedByCount = y2pId_count_txtAllRevs.groupByKey().mapValues(lambda x: sorted(x, key=lambda y: y[1], reverse=True)[:10])

        # year, (productId, count, txt_all_revs) relativi ai 10 prodotti più recensiti per ogni anno

        # funzione da applicare ad ogni elemento strutturato come sopra.
        # Per ogni prodotto in un certo anno si stampano le parole (con almeno 4 caratteri) usate in tutte le recensioni
        def print_word_used(year2pId_count_txtAllRevs):
            out = [] # lista di coppie ((year,producId), word)
            for tupla in year2pId_count_txtAllRevs[1]:
                for word in tupla[2].split():
                    if(len(word) >= 4):
                        out.append(((year2pId_count_txtAllRevs[0],tupla[0]),word))
            return out

        y_pId2word = y2pId_count_txtAllRevs_orderedByCount.flatMap(print_word_used)

        # per tutti i 10 prodotti più recensiti nei vari anni, sono stampate le parole (con almeno 4 caratteri) usate nelle recensioni
        # (year,productId), word

        # dato un prodotto in un anno, conto quante volte una parola è usate nelle recensioni
        # ottenendo: [(((year,pId),word),wordcounter),...]
        y_pId2wordcounted = sc.parallelize(list(y_pId2word.countByValue().items()))

        # convertiamo ogni elemento dell'rdd in: ((year,pId),(word,wordcounter))
        y_pId2wordcount = y_pId2wordcounted.map(lambda x: (x[0][0],(x[0][1],x[1])))

        # per ogni prodotto in un certo anno, stampo in maniera ordinata le parole (almeno 4 caratteri) più usate nelle recensioni 
        # 1. eseguo il groupBy sulla chiave (year,productId) => genera una lista di elementi come valore per ogni chiave
        # 2. per ogni valore associato, ogni elemento è (word, wordcounter)
        #   2.1. eseguo il sorted della lista, prendendo per ogni suo elemento il secondo campo (counter) 
        #   2.2. di questa lista ordianta prendo solo i primi 5
        final_out = y_pId2wordcount.groupByKey().mapValues(lambda x: sorted(x, key=lambda y: y[1], reverse=True)[:5])

        final_out.saveAsTextFile(output_folder_path)

if __name__ == "__main__":
    # parsing degli argomenti
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=str)
    parser.add_argument("--output", type=str)

    args = parser.parse_args()
    input_path = args.input
    output_folder_path = args.output
    top_reviewed_products_with_frequent_word_used(input_path,output_folder_path)