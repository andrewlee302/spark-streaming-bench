#!/usr/bin/env bash
# Author : Andrew Lee


SPARK_HOME="/home/nbtest/develop/spark-1.3.0-bin-hadoop2.4"
BENCHMARK_FILE="/home/nbtest/develop/spark-1.3.0-bin-hadoop2.4/spark-benchmark.jar"
if [[ $# -lt 6 ]]; then
    echo "KafkaWordCount <zkQuorum> <group> <topics> <numThreads> <batchInterval(sec)> <print(0) or save(1)> [<windowInterval(sec)>]"
    exit 1
fi

echo "run command:"
echo $SPARK_HOME/bin/spark-submit --class org.apache.spark.microbench.KafkaWordCount --master spark://lingcloud21:7077 file:$BENCHMARK_FILE $@ 2>/dev/null

$SPARK_HOME/bin/spark-submit --class org.apache.spark.microbench.KafkaWordCount --master spark://lingcloud21:7077 file:$BENCHMARK_FILE $@ 2>/dev/null
