#!/usr/bin/env bash
# Author : Andrew Lee

HADOOP_HOME="/home/nbtest/develop/hadoop-2.6.0"
SOURCE_HOME="/home/nbtest/develop/spark-1.3.0-bin-hadoop2.4/gen_data"
HDFS_SOURCE_HOME="hdfs://lingcloud21:9000/spark-stream/data"

count=0
echo $HADOOP_HOME/bin/hadoop fs -rm /spark-stream/data/*
$HADOOP_HOME/bin/hadoop fs -rm /spark-stream/data/*

for i in `seq 1 1000`;do
    count=$((count+1))
    sleep 1
    for f in $SOURCE_HOME/*;do
            echo $HADOOP_HOME/bin/hadoop fs -copyFromLocal $f $HDFS_SOURCE_HOME/`basename $f`-${count}
            $HADOOP_HOME/bin/hadoop fs -copyFromLocal $f $HDFS_SOURCE_HOME/`basename $f`-${count}
    done
done
