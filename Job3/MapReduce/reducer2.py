#!/usr/bin/env python3

import sys

coupleToAffinity = {}
for line in sys.stdin:

    line = line.strip()
    userIds, productId = line.split('\t')

    userId1, userId2 = userIds.split(',')

    if (userId1, userId2) not in coupleToAffinity:
        coupleToAffinity[(userId1, userId2)] = set()
    
    coupleToAffinity[(userId1, userId2)].add(productId)

for couple in coupleToAffinity.keys():
    if len(coupleToAffinity[couple]) < 3:
        continue
    for productId in coupleToAffinity[couple]:
        affinity = "," + productId

    print('{},{}\t{}'.format(couple[0], couple[1], affinity))

