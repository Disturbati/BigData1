#!/bin/bash

# Esegui il primo job MapReduce
$HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/streaming/hadoop-streaming-3.3.4.jar \
    -mapper /Users/davidemolitierno/Repositories/BigData1/Job2/MapReduce/mapper.py \
    -reducer /Users/davidemolitierno/Repositories/BigData1/Job2/MapReduce/reducer.py \
    -input /user/davidemolitierno/input/Reviews.csv \
    -output /user/davidemolitierno/output/review_output.txt

# Controlla se il primo job MapReduce è stato eseguito con successo
if [ $? -eq 0 ]; then
    # Esegui il secondo job MapReduce
    $HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/streaming/hadoop-streaming-3.3.4.jar \
        -mapper /Users/davidemolitierno/Repositories/BigData1/Job2/MapReduce/mapperSorter.py \
        -reducer /Users/davidemolitierno/Repositories/BigData1/Job2/MapReduce/reducerSorter.py \
        -input /user/davidemolitierno/output/review_output.txt \
        -output /user/davidemolitierno/output/ordered_review_output
else
    echo "Errore nell'esecuzione del primo job MapReduce"
fi

hdfs dfs -rm -R /user/davidemolitierno/output/review_output.txt
