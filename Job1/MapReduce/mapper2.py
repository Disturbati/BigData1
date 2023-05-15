#!/usr/bin/env python3

import sys

for line in sys.stdin:
    year_productId, tot_num_revs, text_revs = line.split("\t")

    # lista di tutte le parole relative alle recensioni del productId nell'anno corrente
    words_rev = text_revs.strip().split(" ")

    for word in words_rev:
        word = word.lower()
        if len(word) >= 4:
            # print(year_productId, word, sep="\t")
            print("%s\t%s" % (year_productId, word))