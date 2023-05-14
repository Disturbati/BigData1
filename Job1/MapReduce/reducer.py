#!/usr/bin/env python3

import sys

# mappa per tener conto del numero di recensioni di un productId (dato un anno) e il testo di tutte le recensioni relative
#   chiave = year
#   valore = dict {productId1: (num_rec1, text_rec1),..., productIdN: (num_recN, text_recN)}
year2productIdStats = {}

for line in sys.stdin:
    # year è la chiave, quindi ad ogni reducer arriveranno tutti i record relativi ad un anno
    year, productId, num_review, text_review = line.strip().split("\t")

    # inizializzo il dizionario se necessario
    if year not in year2productIdStats:
        year2productIdStats[year] = {}

    productsCurrYear = year2productIdStats[year] # guardiamo solo i prodotti dell'anno corrente
   
    # se il productId è già presente nel dizionario associato all'anno che sta guardando il reducer corrente
    # allora modifico num_rec e text_rec
    if productId in productsCurrYear:
        num_prod_reviews = productsCurrYear[productId][0] # numero reviews totali per un productId (relativo ad un anno)
        txt_prev_review = productsCurrYear[productId][1]  # testo delle reviews relative ad un productId (relativo ad un anno)
        # aggiorno i dati:
        #   numero_recensioni + 1
        #   append del testo della recensione corrente
        productsCurrYear[productId] = (num_prod_reviews+1, txt_prev_review+' '+text_review) 

    # altrimenti lo aggiungo al dizionario con i valori inizializzati
    else:
        productsCurrYear[productId] = (1,text_review)

# SORTING
for year in year2productIdStats:
    # per ogni anno faccio il sort di tutti i productId rispetto al numero di reviews e ne prendo i primi 10
    year2productIdStats[year] = {k: v for k,v in sorted(year2productIdStats[year].items(), key=lambda x: x[1], reverse=True)[:10]}

# OUTPUT
for year in year2productIdStats:
    productsCurrYear = year2productIdStats[year]
    for productId in productsCurrYear:
        # stampati per ogni anno i 10 prodotti più recensiti:
        # year, productId, num_reviews totali relative a quel prodotto in quel anno, testo di tutte le reviews di quel prodotto in quel anno
        # print(year, productId, productsCurrYear[productId][0], productsCurrYear[productId][1], sep="\t")
        print("%s,%s\t%s\t%s" % (year, productId, productsCurrYear[productId][0], productsCurrYear[productId][1]))
        # print(year, productId, productsCurrYear[productId][0], sep="\t")