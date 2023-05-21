#!/usr/bin/env python3

import sys

productToUser = {}
for line in sys.stdin:

    line = line.strip()
    productId, userId = line.split('\t')

    if productId not in productToUser:
        productToUser[productId] = set()

    productToUser[productId].add(userId)

# generate all the couples of users that have affinity for the same product
for productId in productToUser:
    for i in range(len(productToUser[productId])):
        for j in range(i+1, len(productToUser[productId])):
            print('{},{}\t{}'.format(productToUser[productId][i], productToUser[productId][j], productId))