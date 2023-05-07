#!/bin/bash

# save all the csv file in dataset folder in the input folder in hdfs
hdfs dfs -put dataset/*.csv  input

