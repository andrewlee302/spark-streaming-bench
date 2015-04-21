/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.microbench

import java.util.Properties

import kafka.producer._

import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf

/**
 * Consumes messages from one or more topics in Kafka and does wordcount.
 * Usage: KafkaWordCount <zkQuorum> <group> <topics> <numThreads>
 *   <zkQuorum> is a list of one or more zookeeper servers that make quorum
 *   <group> is the name of kafka consumer group
 *   <topics> is a list of one or more kafka topics to consume from
 *   <numThreads> is the number of threads the kafka consumer should use
 *
 * Example:
 *    1. 添加SPARK_CLASSPATH指示所有依赖包
 *    `$ bin/spark-submit --class org.apache.spark.microbench.KafkaWordCount --master spark://lingcloud21:7077 file:/home/nbtest/develop/spark-1.3.0-bin-hadoop2.4/spark-benchmark.jar args`
 *    args,eg:
 *    lingcloud21:2181 spark-stream stream-test 1 2 0
 *    numThreads 指的是这个topic采用多少个消费者线程读取。消费者线程和kafka中这个topic的partition分区数共同决定如何并发读取。
 *
 */
object KafkaWordCount {
  def main(args: Array[String]) {
    if (args.length < 6) {
      System.err.println("Usage: KafkaWordCount <zkQuorum> <group> <topics> <numThreads> <batchInterval(sec)> <print(0) or save(1)> [<windowInterval(sec)>]")
      System.exit(1)
    }

    val Array(zkQuorum, group, topics, numThreads, batchIntervalStr, flagStr) = args
    val sparkConf = new SparkConf().setAppName("KafkaWordCount")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    // ssc.checkpoint("checkpoint")

    val batchInterval = batchIntervalStr.toInt
    val flag = flagStr.toInt

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
    val words = lines.flatMap(_.split(" "))
    if (args.length == 6) {
      val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _) // numPartition?
      wordCounts.print()
    } else {
      val wordCounts = words.map(x => (x, 1L))
        .reduceByKeyAndWindow(_ + _, _ - _, Seconds(args(7).toInt), Seconds(batchInterval), 2)
      wordCounts.print()
    }
    ssc.start()
    ssc.awaitTermination()
  }
}


/**
 * 1. $KAFKA_HOME/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic <topicName>
 * 2. java -cp <kafka-lib>:spark-benchmark.jar org.apache.spark.microbench args
 * args, eg:
 * lingcloud21:9092 stream-test 1000 10
 */
// Produces some random words between 1 and 100.
object KafkaWordCountProducer {

  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: KafkaWordCountProducer <metadataBrokerList> <topic> " +
        "<messagesPerSec> <wordsPerMessage>")
      System.exit(1)
    }

    val Array(brokers, topic, messagesPerSec, wordsPerMessage) = args

    // Zookeeper connection properties
    val props = new Properties()
    props.put("metadata.broker.list", brokers)
    props.put("serializer.class", "kafka.serializer.StringEncoder")

    val config = new ProducerConfig(props)
    val producer = new Producer[String, String](config)

    // Send some messages
    while(true) {
      val messages = (1 to messagesPerSec.toInt).map { messageNum =>
        val str = (1 to wordsPerMessage.toInt).map(x => scala.util.Random.nextInt(10).toString)
          .mkString(" ")

        new KeyedMessage[String, String](topic, str)
      }.toArray

      producer.send(messages: _*)
      Thread.sleep(1000)
    }
  }

}