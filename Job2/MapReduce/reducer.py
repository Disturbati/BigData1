#!/usr/bin/env python3
"""reducer.py"""
import sys

user_id_2_ratings = {}

for line in sys.stdin:
    line = line.strip()

    user_id, rating = line.split("\t")

    try:
        rating = float(rating)
    except ValueError:
        continue

    if user_id not in user_id_2_ratings:
        user_id_2_ratings[user_id] = rating

    user_id_2_ratings[user_id] = rating

frequencies = user_id_2_ratings.items()
sorted_user_ids_2_ratings = sorted(frequencies, key=lambda x: x[1], reverse=True)

for user_id, ratings in sorted_user_ids_2_ratings:
    print("%s\t%f" % (user_id, ratings))