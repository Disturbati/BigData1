#!/bin/bash

# se non ci sono i parametri in input ritorna errore altrimenti ottiene i parametri
if ! [ $# -eq 5 ] && ! [ $# -eq 7 ] ; then
    echo "Errore: inserire tutti i parametri necessari"
    echo "Usage: ./run.sh <mapper> <reducer> <input> <output> <num_mapreduce> "
    echo "and if <num_mapreduce> is equal to 2 then insert <mapper2> <reducer2>"
    exit 1
else
    mapper=$1
    reducer=$2
    input=$3

    # se viene impostato il numero di mapreduce a 2 allora output diventa uguale 
    # a partial output altrimenti output diventa uguale a $4
    if [ $5 -eq 1 ]; then
        output=$4
    fi
    if [ $5 -eq 2 ]; then
        output="partial_output"
        final_output=$4
        mapper2=$6
        reducer2=$7
    else
        echo "Errore: inserire al max 1 o 2 MapReduce"
        exit 1
    fi
fi



start=$(date +%s)

# Esegui il primo job MapReduce
$HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/streaming/hadoop-streaming-3.3.4.jar \
    -mapper $PWD/$mapper \
    -reducer $PWD/$reducer \
    -input /user/$USER/input/$input \
    -output /user/$USER/output/$output

# Controlla se il primo job MapReduce è stato eseguito con successo e 
# che il quarto parametro sia uguale a 2 
if [ $? -eq 0 ] && [ $5 -eq 2 ] ; then
    # Esegui il secondo job MapReduce
    $HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/streaming/hadoop-streaming-3.3.4.jar \
        -mapper $PWD/$mapper2 \
        -reducer $PWD/$reducer2 \
        -input /user/$USER/output/$output \
        -output /user/$USER/output/$final_output
else
    echo "Errore nell'esecuzione del primo job MapReduce"
fi

end=$(date +%s)
echo "Elapsed Time: $(($end-$start)) seconds"

# Elimina la cartella output
if [ $5 -eq 2 ]; then
    $HADOOP_HOME/bin/hdfs dfs -rm -R /user/$USER/output/$output
fi