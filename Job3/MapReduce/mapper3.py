#!/usr/bin/env python3

import sys
import itertools

for line in sys.stdin:

    userIds, productIds = line.strip().split('\t')

    userId1, userId2 = userIds.split(',')

    products = productIds.split(',')

    # map all the possible combinations of affinity groups of three elements
    combinations = itertools.combinations(products, 3)

    # use , to separate the elements of the combination
    # and use ; to separate the combinations
    for combination in combinations:
        for elem in combination:
            combination_string = elem + ','
        combinations_string = ';' + combination_string

    for combination in combinations:
        print('{},{}\t{}'.format(userId1, userId2, combinations_string))

