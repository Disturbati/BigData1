#!/usr/bin/env python3

import sys

# mappa per tener conto del numero di recensioni di un productId (dato un anno) e il testo di tutte le recensioni relative
#   chiave = productId
#   valore = (numero_recensioni, testo_recensioni)
productId_already_met = {}

for line in sys.stdin:
    # year è la chiave, quindi ad ogni reducer arriveranno tutti i record relativi ad un anno
    year, productId, num_review, text_review = line.strip().split("\t")

    # se il productId è già nella mappa
    if productId in productId_already_met:
        num_prod_reviews = productId_already_met[productId][0] # numero reviews totali per un productId (relativo ad un anno)
        txt_prev_review = productId_already_met[productId][1]  # testo delle reviews relative ad un productId (relativo ad un anno)
        # aggiorno i dati:
        #   numero_recensioni + 1
        #   append del testo della recensione corrente
        productId_already_met[productId] = (num_prod_reviews+1,txt_prev_review+' '+text_review) 

    # productId incontrato la prima volta
    else:
        productId_already_met[productId] = (1,text_review)

# sort di tutti i productId (relativi ad un anno) rispetto al numero di reviews
productId_already_met = sorted(productId_already_met.keys(), key=lambda x: productId_already_met[x][0], reverse=True)

# stampa di tutto i productId relativi ad un anno, del numero di recensioni e del testo delle recensioni
for productId in productId_already_met:
    print(year, productId, productId_already_met[productId][0], productId_already_met[productId][1], sep="\t")

# TODO: 
#   File "/Users/davidegattini/SourceTreeProj/BigData1/Job1/MapReduce/reducer.py", line 32, in <module>
#       print(year, productId, productId_already_met[productId][0], productId_already_met[productId][1], sep="\t")
#       TypeError: list indices must be integers or slices, not str