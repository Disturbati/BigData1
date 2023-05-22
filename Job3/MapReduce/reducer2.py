#!/usr/bin/env python3

import sys

coupleToAffinity = {}
for line in sys.stdin:

    line = line.strip()
    userIds, productId = line.split('\t')

    if userIds not in coupleToAffinity:
        coupleToAffinity[userIds] = set()
    
    coupleToAffinity[userIds].add(productId)

for couple in coupleToAffinity.keys():
    if len(coupleToAffinity[couple]) < 3:
        continue
    affinity = ""
    first = True
    for productId in coupleToAffinity[couple]:
        if first:
            affinity = productId
            first = False
        affinity = affinity + "," + productId

    print('{}\t{}'.format(couple, affinity))

