#!/bin/bash

# create a folder for each csv file in dataset folder
for file in dataset/*.csv
do
    # get the name of the file
    filename=$(basename -- "$file")
    # remove the extension of the file
    filename="${filename%.*}"
    # create a folder with the name of the file
    hdfs dfs -mkdir input/$filename
    # move the file in the folder
    hdfs dfs -put $file input/$filename
done