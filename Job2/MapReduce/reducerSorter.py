#!/usr/bin/env python3
"""reducer.py"""
import sys

for line in sys.stdin:
    
    line = line.strip()

    rating, user_id = line.split("\t")

    print("%s\t%s" % (user_id, rating))