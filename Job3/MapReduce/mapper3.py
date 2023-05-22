#!/usr/bin/env python3

import sys
import itertools

for line in sys.stdin:

    userIds, productIds = line.strip().split('\t')

    products = productIds.split(',')

    # map all the possible combinations of affinity groups of three elements
    combinations = itertools.combinations(products, 3)

    # use , to separate the elements of the combination
    # and use ; to separate the combinations
    combinations_string = ""
    first = True
    for combination in combinations:
        firstElem = True
        for elem in combination:
            if firstElem:
                combination_string = elem
                firstElem = False
            else:
                combination_string = combination_string + "," + elem
            
        if first:
            combinations_string = combination_string
            first = False
        else:
            combinations_string = combinations_string + ";" + combination_string

    print('{}\t{}'.format(userIds, combinations_string))

