#!/bin/bash

JAR=kmeans.jar
OUT=kmeans.out
IN=data.csv

hdfs dfs -rm -r $OUT
hdfs dfs -rm -r Iteration*
hdfs dfs -rm -r kmeans.out

spark-submit --verbose --master yarn-cluster --class bigdata.ml.App $JAR $IN 1000 20 $OUT

