#!/bin/bash

# input: hdfs:/<ip>.ec2.internal:8020/input/$2
# output: hdfs:/<ip>.ec2.internal:8020/output/$3

if [[ $# -ne 3 ]]
    then
        echo "Usage: run.sh <spark_python_file> <input_file> <output_file>"
        exit 1
fi

# check time elapsed
start=`date +%s`

# esegui spark-submit
spark-submit --master yarn $1 --input $2 --output $3

# check time elapsed
end=`date +%s`
runtime=$((end-start))
echo "Time elapsed: $runtime seconds"