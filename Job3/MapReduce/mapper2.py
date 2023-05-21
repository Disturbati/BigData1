#!/usr/bin/env python3

import sys

for line in sys.stdin:

    userIds, productId = line.strip().split('\t')

    userId1, userId2 = userIds.split(',')

    print('{},{}\t{}'.format(userId1, userId2, productId))

