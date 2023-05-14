#!/usr/bin/env python3

import sys

for line in sys.stdin:
    year_productId, tot_num_revs, text_revs = line.split("\t")
    year_productId = year_productId.strip()

    # lista di tutte le parole relative alle recensioni del productId nell'anno corrente
    words_rev = text_revs.strip().split(" ")

    for word in words_rev:
        word = word.lower().strip()
        if len(word) >= 4:
            print(year_productId, word, sep="\t")