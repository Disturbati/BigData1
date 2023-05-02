# Corso di Big Data: Primo Progetto 26 Aprile 2023

## Risorse: [dataset](https://www.kaggle.com/datasets/snap/amazon-fine-food-reviews)
- 500.000 recensioni di prodotti gastronomici rilasciati su Amazon dal 1999 al 2012
- Formato CSV
- Schema

| Id  | ProductId | UserId | ProfileName | HelpfulnessNumerator | HelpfulnessDenominator | Score | Time | Summary | Text |
| --- | --------- | ------ | ----------- | -------------------- | ---------------------- | ----- | ---- | ------- | ---- |

P.S. Scaricare il file dal link (di cui sopra) e inserirlo nella cartella [dataset](./dataset/)

## Obiettivi:
1. Prepara il dataset (eliminando eventuali dati errati o non significativi)
2. Esegui almeno uno dei seguenti job in MapReduce, Hive e Spark:
   1. Un job che sia in grado di generare, per ciascun anno, i 10 prodotti che hanno ricevuto il maggior numero di recensioni e, per ciascuno di essi, le 5 parole con almeno 4 caratteri più frequentemente usate nelle recensioni (campo text), indicando, per ogni parola, il numero di occorrenze della parola.
   2. Un job che sia in grado di generare una lista di utenti ordinata sulla base del loro apprezzamento, dove l’apprezzamento di ogni utente è ottenuto dalla media dell’utilità (rapporto tra HelpfulnessNumerator e HelpfulnessDenominator) delle recensioni che hanno scritto, indicando per ogni utente il loro apprezzamento.
   3. Un job in grado di generare gruppi di utenti con gusti affini, dove gli utenti hanno gusti affini se hanno recensito con score superiore o uguale a 4 almeno 3 prodotti in comune, indicando, per ciascun gruppo, i prodotti condivisi. Il risultato deve essere ordinato in base allo UserId del primo elemento del gruppo e non devono essere presenti duplicati.
3. Illustra e documento il lavoro svolto in un rapporto finale:
      - e operazioni di preparazione dei dati che sono state eventualmente effettuate
      - Una possibile implementazione dei job sopra indicati in MapReduce (pseudocodice), Hive, Spark core
        (pseudocodice) e SparkSQL.
      - Le prime 10 righe dei risultati dei vari job.
      - Tabella e grafici di confronto dei tempi di esecuzione in locale e su cluster dei vari job con dimensioni
        crescenti dell’input.
      - Il relativo codice completo MapReduce e Spark (da allegare al documento)

N.B. *Per aumentare le dimensioni dell’input si suggerisce di generare copie del file dato, eventualmente alterando alcuni dati.*

## Data di consegna: **entro il 25 Maggio 2023**
