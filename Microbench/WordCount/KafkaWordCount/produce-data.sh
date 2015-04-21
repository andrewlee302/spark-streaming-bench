#!/usr/bin/env bash
# Author : Andrew Lee

JAVA="/usr/local/jdk1.7.0_25/bin/java"
KAFKA_HOME="/home/nbtest/develop/kafka_2.10-0.8.2.1"
BENCHMARK_FILE="/home/nbtest/develop/spark-1.3.0-bin-hadoop2.4/spark-benchmark.jar"
if [[ $# -lt 4 ]]; then
    echo "KafkaWordCountProducer <metadataBrokerList> <topic> <messagesPerSec> <wordsPerMessage>"
    exit 1
fi

echo "run command:"
echo $JAVA -cp $(echo $KAFKA_HOME/libs/* | tr " " ":"):$BENCHMARK_FILE org.apache.spark.microbench.KafkaWordCountProducer $@
$JAVA -cp $(echo $KAFKA_HOME/libs/* | tr " " ":"):$BENCHMARK_FILE org.apache.spark.microbench.KafkaWordCountProducer $@
