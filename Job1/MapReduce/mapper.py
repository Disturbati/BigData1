#!/usr/bin/env python3

import sys
import csv
from datetime import datetime
import re
import string

reader = csv.reader(sys.stdin, delimiter=",", quotechar="\"")
next(reader) # skippa l'header

for line in reader:
    # prendo l'anno
    unix_timestamp = line[7]
    try:
        year = datetime.fromtimestamp(int(unix_timestamp)).year
    except:
        with open("./except.txt", "w") as f:
            f.write(unix_timestamp)
        continue
    
    # prendo il productId
    productId = line[1]

    # prendo il testo
    text_review = line[9]

    # pre processing del testo, pulito da punteggiatura e tag html
    html_regex = re.compile(r"<.*?>") # regex per togliere tag html
    text_review = html_regex.sub("", text_review) # esegue sostutuzione

    # rimuovi la punteggiatura
    text_review = text_review.translate(str.maketrans(string.punctuation, ' '*len(string.punctuation)))

    # rimuovi tutti i caratteri di spaziatura
    text_review = " ".join(text_review.split())

    print(year, productId, 1, text_review, sep="\t")