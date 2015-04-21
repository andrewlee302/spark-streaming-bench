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

import java.util.ArrayList

import akka.actor.{Actor, ActorRef, Props, actorRef2Scala}
import org.apache.spark.streaming.receiver.ActorHelper
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.util.AkkaUtils
import org.apache.spark.{SecurityManager, SparkConf}

import scala.collection.mutable.LinkedList
import scala.reflect.ClassTag
import scala.util.Random

case class SubscribeReceiver(receiverActor: ActorRef)
case class UnsubscribeReceiver(receiverActor: ActorRef)

class FeederActor (messageSize: Int, messageNumPerSecond: Int) extends Actor{
  var sleepMilliSecond = 0
  var sleepNanoSecond = 0
  if(messageNumPerSecond <= 1000){
    sleepMilliSecond = 1000 / messageNumPerSecond
  } else {
    sleepNanoSecond = 1000*1000*1000 / messageNumPerSecond
  }
  println("sleepMilliSecond = " + sleepMilliSecond + "; sleepNanoSecond = " + sleepNanoSecond)
  val rand = new Random()
  var receivers: LinkedList[ActorRef] = new LinkedList[ActorRef]()

  val strings: Array[String] = Array("word ", "node ", "cake ", "have ", "cook ", "cock ", "done ", "time ", "list ", "When ")

  val messages: ArrayList[String] = new ArrayList[String]
  val wordNum:Int = messageSize/5

  var i = 0
  for(i <- 1 to 20) {
    val tmp = makeMessage(wordNum)
    messages.add(tmp)
  }

  def makeMessage(wordNum: Int): String = {
    val str:StringBuilder = new StringBuilder
    var i = 0
    for(i <- 1 to wordNum) {
      val x = rand.nextInt(strings.length)
      str++=(strings(x))
    }
    return str.toString()
  }

  def getMessage(): String = {
    messages.get(rand.nextInt(messages.size()))
  }

  /*
   * A thread to generate random messages
   */
  new Thread() {
    override def run() {
      while (true) {
        Thread.sleep(sleepMilliSecond, sleepNanoSecond)
        receivers.foreach(_ ! getMessage())
      }
    }
  }.start()

  def receive: Receive = {

    case SubscribeReceiver(receiverActor: ActorRef) =>
      println("received subscribe from %s".format(receiverActor.toString))
      receivers = LinkedList(receiverActor) ++ receivers

    case UnsubscribeReceiver(receiverActor: ActorRef) =>
      println("received unsubscribe from %s".format(receiverActor.toString))
      receivers = receivers.dropWhile(x => x eq receiverActor)

  }
}

/**
 * A sample actor as receiver, is also simplest. This receiver actor
 * goes and subscribe to a typical publisher/feeder actor and receives
 * data.
 *
 */
class SampleActorReceiver[T: ClassTag](urlOfPublisher: String)
  extends Actor with ActorHelper {

  lazy private val remotePublisher = context.actorSelection(urlOfPublisher)

  override def preStart = remotePublisher ! SubscribeReceiver(context.self)

  def receive = {
    case msg => store(msg.asInstanceOf[T])
  }

  override def postStop() = remotePublisher ! UnsubscribeReceiver(context.self)

}


object FeederActor {

  def main(args: Array[String]) {
    if(args.length < 4){
      System.err.println(
        "Usage: FeederActor <hostname> <port> <messageSize(B)> <messageNumPerSecond>"
      )
      System.exit(1)
    }
    val Seq(host, port, messageSizeStr, messageNumPerSecondStr) = args.toSeq
    val messageSize = messageSizeStr.toInt
    val messageNumPerSecond = messageNumPerSecondStr.toInt

    val conf = new SparkConf
    val actorSystem = AkkaUtils.createActorSystem("spout", host, port.toInt, conf = conf,
      securityManager = new SecurityManager(conf))._1
    val feeder = actorSystem.actorOf(Props(new FeederActor(messageSize, messageNumPerSecond)), "FeederActor")

    println("Feeder started as:" + feeder)

    actorSystem.awaitTermination()
  }
}

/**
 *
 * To run this example locally, you may run Feeder Actor as
 *    `$ bin/spark-submit --class org.apache.spark.microbench.FeederActor --master spark://lingcloud21:7077 file:/home/nbtest/develop/spark-1.3.0-bin-hadoop2.4/spark-benchmark.jar <hostname> <port> <messageSize> <messageNumPerSecond>`
 * and then run the example
 *    `$ bin/spark-submit --class org.apache.spark.microbench.ActorWordCount --master spark://lingcloud21:7077 file:/home/nbtest/develop/spark-1.3.0-bin-hadoop2.4/spark-benchmark.jar <hostname> <port> <batchInterval> <print(0) or save(1)>`
 */
object ActorWordCount {
  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println(
        "Usage: ActorWordCount <hostname> <port> <batchInterval(sec)> <print(0) or save(1)>")
      System.exit(1)
    }

    val Seq(host, port, batchIntervalStr, flagStr) = args.toSeq
    val batchInterval = batchIntervalStr.toInt
    val flag = flagStr.toInt
    val sparkConf = new SparkConf().setAppName("ActorWordCount")
    // Create the context and set the batch size
    val ssc = new StreamingContext(sparkConf, Seconds(batchInterval))

    /*
     * Following is the use of actorStream to plug in custom actor as receiver
     *
     * An important point to note:
     * Since Actor may exist outside the spark framework, It is thus user's responsibility
     * to ensure the type safety, i.e type of data received and InputDstream
     * should be same.
     *
     * For example: Both actorStream and SampleActorReceiver are parameterized
     * to same type to ensure type safety.
     */


    val lines = ssc.actorStream[String](
      Props(new SampleActorReceiver[String]("akka.tcp://spout@%s:%s/user/FeederActor".format(
        host, port.toInt))), "SampleReceiver")

    val wordCounts = lines.flatMap(_.split("\\s+")).map(x => (x, 1)).reduceByKey(_ + _)
    if(0 == flag)
      wordCounts.print()
    else if(1 == flag)
      wordCounts.saveAsTextFiles("/spark-stream/out/result")
    else
      return
    ssc.start()
    ssc.awaitTermination()
  }
}
