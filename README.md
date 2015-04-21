# spark-streaming-bench

## Add the metrics for Spark
You can find the metrics configuration template on the [project web page](https://github.com/apache/spark/blob/master/conf/metrics.properties.template). Note you have to add the driver.sink at least, because the driver metrics contain every batch's info.


## MicroBench
### WordCount
* ActorWordCount
* HDFSWordCount
* KafkaWordCount

### TopK(TODO)
* HDFSTopK
* KafkaTopK

## Dependency Library
* kafka-clients-0.8.2.1.jar
* kafka_2.10-0.8.2.1.jar
* metrics-core-2.2.0.jar
* spark-assembly-1.3.0-hadoop2.4.0.jar
* spark-streaming-kafka_2.10-1.3.0.jar
* zkclient-0.3.jar

## The script is hard-coding, I will do code refine and add the help info then.

## Contact: Andrew Lee ([lichundian@gmail.com](mailto:lichundian@gmail.com))
