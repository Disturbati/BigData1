#!/usr/bin/env python3

import sys
import csv
from datetime import datetime

reader = csv.reader(sys.stdin, delimiter=",", quotechar="\"")

for line in reader:
    
    # prendo l'anno
    unix_timestamp = line[7]
    try:
        year = datetime.fromtimestamp(int(unix_timestamp)).year
    except:
        # TODO: FAI PRINT PER DEBUGGING E VEDERE QUAL è IL PROBLEMA
        # print(unix_timestamp, file="./log.txt")
        continue
    
    # prendo il productId
    productId = line[1]

    # prendo il testo
    text_review = line[9]

    print(year, productId, 1, text_review, sep="\t")