#!/bin/bash

# se non ci sono i parametri in input ritorna errore altrimenti ottiene i parametri
if ! [ $# -eq 5 ] && ! [ $# -eq 7 ] && ! [ $# -eq 9 ] ; then
    echo "Errore: inserire tutti i parametri necessari"
    echo "Usage: ./run.sh <mapper> <reducer> <input> <output> <num_mapreduce> "
    echo "and if <num_mapreduce> is equal to 2 then insert <mapper2> <reducer2>"
    echo "and if <num_mapreduce> is equal to 3 then insert <mapper2> <reducer2> <mapper3> <reducer3>"
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
        if [ $5 -eq 3 ]; then
            output="partial_output"
            final_output="partial_output2"
            final_output2=$4
            mapper2=$6
            reducer2=$7
            mapper3=$8
            reducer3=$9
        else
            echo "Errore: inserire al max 1 o 2 MapReduce"
            exit 1
        fi
    fi
fi



start=$(date +%s)

# Esegui il primo job MapReduce
hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar \
    -file $PWD/$mapper -mapper $PWD/$mapper \
    -file $PWD/$reducer -reducer $PWD/$reducer \
    -input /input/$input \
    -output /output/$output

# Controlla se il primo job MapReduce è stato eseguito con successo e 
# che il quarto parametro sia uguale a 2 
if [ $? -eq 0 ] && [ $5 -eq 2 ] ; then
    # Esegui il secondo job MapReduce
    hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar \
        -file $PWD/$mapper2 -mapper $PWD/$mapper2 \
        -file $PWD/$reducer2 -reducer $PWD/$reducer2 \
        -input /output/$output \
        -output /output/$final_output
else
    echo "Errore nell'esecuzione del primo job MapReduce"
fi

if [ $? -eq 0 ] && [ $5 -eq 3 ] ; then
    # Esegui il secondo job MapReduce
    hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar \
        -file $PWD/$mapper2 -mapper $PWD/$mapper2 \
        -file $PWD/$reducer2 -reducer $PWD/$reducer2 \
        -input /output/$output \
        -output /output/$final_output

    # Esegui il terzo job MapReduce
    if [ $? -eq 0 ] ; then
        hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar \
            -file $PWD/$mapper3 -mapper $PWD/$mapper3 \
            -file $PWD/$reducer3 -reducer $PWD/$reducer3 \
            -input /output/$final_output \
            -output /output/$final_output2
    else
        echo "Errore nell'esecuzione del secondo job MapReduce"
    fi
else
    echo "Errore nell'esecuzione del primo job MapReduce"
fi

end=$(date +%s)
echo "Elapsed Time: $(($end-$start)) seconds"

# Elimina la cartella output
if [ $5 -eq 2 ]; then
    hdfs dfs -rm -R /output/$output
fi

if [ $5 -eq 3 ]; then
    hdfs dfs -rm -R /output/$output
    hdfs dfs -rm -R /output/$final_output
fi