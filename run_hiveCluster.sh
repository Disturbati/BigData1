#!/usr/bin/env bash

# ARGUMENTI:
# 1. csv dataset file used as input (e.g. Reviews_2)
# 2. .hql path to run

if [ $# -lt 2 ]
  then
    echo ""
    echo "Please provide:"
    echo "  1) csv dataset file used as input (e.g. Reviews_2)" # Path assoluto
    echo "  2) .hql path to run"                                # Path relativo
    echo ""
    exit;
fi

# calcolo il path assoluto dello script
PROC_HIVE_PATH=$(readlink -f "$2")

# calcola la cartella in cui si trova il file passato come primo parametro
HIVE_FOLDER=$(dirname "$PROC_HIVE_PATH")

echo "Script path: "$PROC_HIVE_PATH
echo "Hive folder: "$HIVE_FOLDER

DATE=$(date +"%Y%m%d_%H%M%S")

echo "Executing hive script..."
echo "-----------------------------"
echo ""

# ======================LANCIA HIVE==============================

START=$(date +%s);

hive --hiveconf dbName="$1" -f $PROC_HIVE_PATH

END=$(date +%s);

# ================================================================
echo "Time elapsed: "
echo $((END-START)) | awk '{print int($1/60)":"int($1%60)}'

echo "-----------------------------"
echo ""