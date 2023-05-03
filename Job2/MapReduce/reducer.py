#!/usr/bin/env python3
"""reducer.py"""
import sys

user_id_2_ratings = {}

for line in sys.stdin:
    
    line = line.strip()

    user_id, rating = line.split("\t")

    if user_id not in user_id_2_ratings:
        user_id_2_ratings[user_id] = (0, 0)
    
    try:
        user_id_2_ratings[user_id][0] += float(rating)
    except ValueError:
        continue

    user_id_2_ratings[user_id][1] += 1

for user_id in user_id_2_ratings.keys():
    print("%s\t%f" % (user_id, user_id_2_ratings[user_id][0] / user_id_2_ratings[user_id][1]))