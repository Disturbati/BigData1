#!/usr/bin/env python3

import sys

affinityToGroup = {}
for line in sys.stdin:

    line = line.strip()
    userIds, affinities = line.split('\t')

    userId1, userId2 = userIds.split(',')

    affinities = affinities.split(';')

    affinities_list = []

    for affinity in affinities:
        affinity_set = tuple(affinity.split(',')) 
        if affinity_set not in affinityToGroup:
            affinityToGroup[affinity_set] = set()
        
        affinityToGroup[affinity_set].add(userId1)
        affinityToGroup[affinity_set].add(userId2)
    
# sort the map by the first element of the users
sorted_affinityToGroup = dict(sorted(affinityToGroup.items(), key=lambda x: list(x[0])[0]))

for affinity in sorted_affinityToGroup.keys():

    affinity_string = ""
    group_string = ""

    first = True
    for product in affinity:
        if first:
            affinity_string = product
            first = False
        affinity_string = affinity_string + "," + product

    first = True
    for user in sorted_affinityToGroup[affinity]:
        if first:
            group_string = user
            first = False
        group_string = group_string + "," + user

    print('{}\t{}'.format(affinity_string, group_string))