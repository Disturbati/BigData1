#!/bin/bash

if [[ $# -ne 2 ]]
    then
        echo "Usage: run.sh <spark_python_file> <input_file>"
        echo "Note: <input_file> must be in hdfs:///user/$USER/input/"
        exit 1
fi

# esegui spark-submit
spark-submit --master yarn $1 --input hdfs:///user/$USER/input/$2