# spark-streaming-bench

## Add the metrics for Spark
You can find the metrics configuration template on the [project web page](https://github.com/apache/spark/blob/master/conf/metrics.properties.template). Note you have to add the driver.sink at least, because the driver metrics contain every batch's info.


## MicroBench
1. WordCount
    ActorWordCount
    HDFSWordCount(TODO)
    KafkaWordCount(TODO)

2. TopK(TODO)
    HDFSTopK
    KafkaTopK
