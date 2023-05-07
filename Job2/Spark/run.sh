#!/bin/bash

# esegui spark-submit
spark-submit --master yarn sortedAppreciateSQL.py --input hdfs:///user/$USER/input/$1