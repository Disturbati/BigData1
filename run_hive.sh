#!/usr/bin/env bash

# @TODO: 
#   1. Da creare la cartella derby-db-schema nella root del progetto 
#   2. Posizionati nella cartella derby-db-schema 
#   3. Lancia schematool -type derby -initSchema
#   4. Runna lo script run_hive.sh rimanendo nella cartella derby-db-schema

# ARGUMENTI:
# 1. hdfs username
# 2. csv dataset file used as input (e.g. Reviews_2)
# 1. .hql path to run
# 2. .hql path to show first 10 rows of the output
# 3. .hql path to delete the table (optional)

if [ $# -lt 4 ]
  then
    echo ""
    echo "Please provide:"
    echo "  1) hdfs username"
    echo "  2) csv dataset file used as input (e.g. Reviews_2)"
    echo "  3) .hql path to run"
    echo "  4) .hql path to show first 10 rows of the output"
    echo "  (if you want to delete the table, please provide also .hql path to delete the table)"
    echo ""
    exit;
fi

if [ $# -eq 4 ]
  then
    echo ""
    echo "Do you want to delete the table? (y/n)"
    read answer

    if [[ "$answer" = "y" || "$answer" = "Y" ]]
      then
        echo ""
        echo "Please provide:"
        echo "  1) hdfs username"
        echo "  2) csv dataset file used as input (e.g. Reviews_2)"
        echo "  3) .hql path to run"
        echo "  4) .hql path to show first 10 rows of the output"
        echo "  5) .hql path to delete the table"
        echo ""
        exit;
    fi
fi

# calcolo il path assoluto dello script
PROC_HIVE_PATH=$(readlink -f "$3")
OUTPUT_HIVE_PATH=$(readlink -f "$4")
DELETE_HIVE_PATH=$(readlink -f "$5")

# calcola la cartella in cui si trova il file passato come primo parametro
HIVE_FOLDER=$(dirname "$PROC_HIVE_PATH")

echo "Script path: "$PROC_HIVE_PATH
echo "Hive folder: "$HIVE_FOLDER

DATE=$(date +"%Y%m%d_%H%M%S")

LOGFILE="$HIVE_FOLDER/log/"$DATE".log"

echo "Executing hive script..."
echo "-----------------------------"
echo ""

cd "derby-db-schema"

# ======================LANCIA HIVE==============================

START=$(date +%s);

hive --hiveconf username="$1" --hiveconf regexDB="$2" -f $PROC_HIVE_PATH

END=$(date +%s);

# ================================================================

echo $((END-START)) | awk '{print int($1/60)":"int($1%60)}'

# stampa il tempo di esecuzione del job in un file di log
echo $((END-START)) | awk '{print int($1/60)":"int($1%60)}' >> $LOGFILE

echo "-----------------------------"
echo ""

echo "First 10 rows of the output:"
echo "-----------------------------"
echo ""
hive --hiveconf username="$1" --hiveconf regexDB="$2" -f $OUTPUT_HIVE_PATH
echo "-----------------------------"
echo ""

# se c'è un terzo parametro, cancello le tabelle
if [ $# -eq 5 ]
  then
    echo "Deleting table..."
    echo "-----------------------------"
    echo ""
    hive -f $DELETE_HIVE_PATH
    echo "-----------------------------"
    echo ""
fi

exit;