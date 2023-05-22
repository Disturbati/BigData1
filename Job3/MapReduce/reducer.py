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
    users_list = list(productToUser[productId])
    for i in range(len(users_list)):
        for j in range(i+1, len(users_list)):
            print('{},{}\t{}'.format(users_list[i], users_list[j], productId))