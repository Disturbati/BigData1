#!/usr/bin/env python3
"""mapper.py"""

import sys 

for line in sys.stdin:

    line = line.strip()

    user_id, meanOfRatings = line.split("\t")

    print("%s\t%s" % (meanOfRatings, user_id))