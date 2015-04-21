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


import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 *
 * To run this example locally, you may run Feeder Actor as
 *    `$ bin/spark-submit --class org.apache.spark.microbench.HdfsWordCount --master spark://lingcloud21:7077 file:/home/nbtest/develop/spark-1.3.0-bin-hadoop2.4/spark-benchmark.jar <directory>  <batchInterval> <print(0) or save(1)>`
 */
object HdfsWordCount {
  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println("Usage: HdfsWordCount <directory>  <batchInterval(sec)> <print(0) or save(1)>")
      System.exit(1)
    }
    val Seq(dir, batchIntervalStr, flagStr) = args.toSeq
    val batchInterval = batchIntervalStr.toInt
    val flag = flagStr.toInt

    val sparkConf = new SparkConf().setAppName("HdfsWordCount")
    val ssc = new StreamingContext(sparkConf, Seconds(batchInterval))

    // Create the FileInputDStream on the directory and use the
    // stream to count words in new files created
    val lines = ssc.textFileStream(dir)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    if(flag == 0)
      wordCounts.print()
    else if(flag == 1)
      wordCounts.saveAsTextFiles("/spark-stream/out/result")
    else
      return
    ssc.start()
    ssc.awaitTermination()
  }
}