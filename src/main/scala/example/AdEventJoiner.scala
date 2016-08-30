/*
 * Copyright 2016 and onwards Neera Agarwal.
 *
 */
package example

import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming.{Seconds, StreamingContext}

case class AdEvent(queryId: Int, adId: Int, timeStamp: Long)
case class ClickEvent(queryId: Int, clickId: Int, timeStamp: Long)

object AdEventJoiner {

  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("AdClickCounter").setMaster("local[3]")

    // Create spark context with window of 10 seconds.
    val ssc = new StreamingContext(sparkConf, Seconds(10))

    // Set checkpoint directory to use the stateful transformations. Checkpoint directory allow for periodic RDD checkpointing.
    ssc.checkpoint("checkpoint-tutorial")

    val rootLogger = Logger.getRootLogger
    rootLogger.setLevel(Level.ERROR)

    val adStreamTopicSet = "ad-stream-topic".split(",").toSet
    val clickStreamTopicSet = "click-stream-topic".split(",").toSet

    val kafkaParams = Map[String, String]("metadata.broker.list" -> "localhost:9092")

    //Connects Spark Streaming to Kafka Topic and gets DStream of RDDs (ad event message)
    val adStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, adStreamTopicSet)

    //Create a new DStream by extracting kafka message and converting it to DStream[queryId, AdEvent]
    val adEventDStream =  adStream.transform( rdd => {
        rdd.map(line => line._2.split(",")).
          map(row => (row(0).trim.toInt, AdEvent(row(0).trim.toInt, row(1).trim.toInt, row(2).trim.toLong)))
    })

    //Connects Spark Streaming to Kafka Topic and gets DStream of RDDs (click event message)
    val clickStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, clickStreamTopicSet)

    //Create a new DStream by extracting kafka message and converting it to DStream[queryId, ClickEvent]
    val clickEventDStream = clickStream.transform{ rdd =>
      rdd.map(line => line._2.split(",")).
        map(row => (row(0).trim.toInt, ClickEvent(row(0).trim.toInt, row(1).trim.toInt, row(2).trim.toLong)))
    }

    // Join adEvent and clickEvent DStreams and output DStream[queryId, (adEvent, clickEvent)]
    val joinByQueryId = adEventDStream.join(clickEventDStream)
    joinByQueryId.print()

    // Transform DStream to DStream[adId, count(adId)] for each RDD
    val countByAdId = joinByQueryId.map(rdd => (rdd._2._1.adId,1)).reduceByKey(_+_)

    // Update the state [adId, countCummulative(adId)] by values from the next RDDs
    val updateFunc = (values: Seq[Int], state: Option[Int]) => {
      val currentCount = values.sum
      val previousCount = state.getOrElse(0)
      Some(currentCount + previousCount)
    }
    val countByAdIdCumm = countByAdId.updateStateByKey(updateFunc)

    // Transform (key, value) pair to (adId, count(adId), countCummulative(adId))
    val ad = countByAdId.join(countByAdIdCumm).
      map {case (adId, (count, cumCount)) => (adId, count, cumCount)}

    // Print report
    ad.foreachRDD( ad => {
      println("%5s %10s %12s".format("AdId", "AdCount", "AdCountCumm"))
        ad.foreach( row => println("%5s %10s %12s".format(row._1,  row._2,  row._3)))
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
