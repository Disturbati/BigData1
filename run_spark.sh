#!/bin/bash

if [[ $# -ne 3 ]]
    then
        echo "Usage: run.sh <spark_python_file> <input_file> <output_file>"
        echo "Note: <input_file> must be in hdfs:///user/$USER/input/"
        exit 1
fi

# esegui spark-submit
spark-submit --master yarn $1 --input hdfs:///user/$USER/input/$2 --output hdfs:///user/$USER/output/$3