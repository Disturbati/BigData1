#!/usr/bin/env python3

import sys

# chiave: (year, productId)
# valore: {word1: count1, ..., wordN: countN}
year_productId2wordcount = {} # conta occorrenze delle parole per il reducer corrente (dato year e productId)
for line in sys.stdin:
    line = line.strip() # necessario senno stampa righe vuote
    year_productId, word = line.split("\t")
    year, productId = year_productId.split(",")

    # inizializza il dizionario {word: count}
    if year_productId not in year_productId2wordcount:
        year_productId2wordcount[year_productId] = {}

    # dizionario {word1: coun1, ..., wordN: countN} relativo ad una coppia (year, productId)
    word2count = year_productId2wordcount[year_productId]

    # aggiorna il contatore relativo alle parole nel dizionario
    if word not in word2count:
        word2count[word] = 1
    else:
        word2count[word] += 1
        
# SORTING
for year_productId in year_productId2wordcount:
    # per ogni (year, productId) ordino le parole con almeno 4 caratteri più usate e prendo le prime 5
    year_productId2wordcount[year_productId] = {k:v for k,v in sorted(year_productId2wordcount[year_productId].items(), key=lambda x: x[1], reverse=True)[:5]}
    
# OUTPUT
for year_productId in year_productId2wordcount:
    year, productId = year_productId.split(",")
    top_5_word2count = year_productId2wordcount[year_productId]
    for top_5_word in top_5_word2count:
        # print(year, productId, top_5_word, top_5_word2count[top_5_word], sep="\t")
        print("%s\t%s\t%s\t%i" % (year, productId, top_5_word, top_5_word2count[top_5_word]))