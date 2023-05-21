#!/usr/bin/env python3

import sys
import csv

reader = csv.reader(sys.stdin, delimiter=',', quotechar='"')
next(reader)

for line in reader:

    productId = line[1]
    userId = line[2]
    score = int(line[6])

    if score < 4:
        continue

    print('{}\t{}'.format(productId, userId))
