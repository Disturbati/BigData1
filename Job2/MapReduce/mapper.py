#!/usr/bin/env python3
"""mapper.py"""

import sys 
import csv

reader = csv.reader(sys.stdin, delimiter=',')

for line in reader:

    user_id = line[2]
    
    try:
        helpfulness_numerator = int(line[4])
        helpfulnes_denominator = int(line[5])
    except ValueError:
        continue

    if (helpfulness_numerator < 0 or helpfulnes_denominator <= 0 or helpfulness_numerator > helpfulnes_denominator):
        continue

    rating = helpfulness_numerator / helpfulnes_denominator

    print("%s\t%f" % (user_id, rating))


