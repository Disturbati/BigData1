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
        affinity_set = set(affinity.split(',')) 
        if affinity_set not in affinityToGroup:
            affinityToGroup[affinity_set] = set()
        
        affinityToGroup[affinity_set].add(userId1)
        affinityToGroup[affinity_set].add(userId2)
    
# sort the map by the first element of the users
sorted_affinityToGroup = sorted(affinityToGroup.items(), key=lambda x: x[0])

for affinity in sorted_affinityToGroup.keys():

    for product in affinity:
        affinity_string = "," + product

    for user in sorted_affinityToGroup[affinity]:
        group_string = "," + user

    print('{}\t{}'.format(affinity_string, group_string))

    

