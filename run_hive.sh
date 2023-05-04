#!/usr/bin/env bash

# @TODO: 
#   1. Da creare la cartella derby-db-schema nella root del progetto 
#   2. Posizionati nella cartella derby-db-schema 
#   3. Lancia schematool -type derby -initSchema
#   4. Runna lo script run_hive.sh rimanendo nella cartella derby-db-schema

# ARGUMENTI:
#  1. .hql path to run
#  2. .hql path to show first 10 rows of the output
#  3. .hql path to delete the table (optional)

if [ $# -lt 2 ]
  then
    echo ""
    echo "Please provide:"
    echo "  1) .hql path to run"
    echo "  2) .hql path to show first 10 rows of the output"
    echo "  (if you want to delete the table, please provide also .hql path to delete the table)"
    echo ""
    exit;
fi

if [ $# -eq 2 ]
  then
    echo ""
    echo "Do you want to delete the table? (y/n)"
    read answer

    if [[ "$answer" = "y" || "$answer" = "Y" ]]
      then
        echo ""
        echo "Please provide:"
        echo "  1) .hql path to run"
        echo "  2) .hql path to show first 10 rows of the output"
        echo "  3) .hql path to delete the table"
        echo ""
        exit;
    fi
fi

# calcolo il path assoluto dello script
SCRIPTPATH=$(readlink -f "$1")

# calcola la cartella in cui si trova il file passato come primo parametro
HIVE_FOLDER=$(dirname "$1")

echo "Script path: "$SCRIPTPATH

DATE=$(date +"%Y%m%d_%H%M%S")

LOGFILE="$HIVE_FOLDER/log/"$DATE".log"

echo "Executing hive script..."
echo "-----------------------------"
echo ""

START=$(date +%s);

hive -f $1

END=$(date +%s);

echo $((END-START)) | awk '{print int($1/60)":"int($1%60)}'

# stampa il tempo di esecuzione del job in un file di log
echo $((END-START)) | awk '{print int($1/60)":"int($1%60)}' >> $LOGFILE

echo "-----------------------------"
echo ""



echo "First 10 rows of the output:"
echo "-----------------------------"
echo ""
hive -f $2
echo "-----------------------------"
echo ""

# se c'è un terzo parametro, cancello le tabelle
if [ $# -eq 3 ]
  then
    echo "Deleting table..."
    echo "-----------------------------"
    echo ""
    hive -f $3
    echo "-----------------------------"
    echo ""
fi

exit;