/*
 * Copyright 2016 and onwards Neera Agarwal.
 *
 */
package example

import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

case class WikiEdit(title: String, flags: String, diffUrl: String, user: String, byteDiff: Int, summary: String)

object WikiPediaStreaming {

  def main(args: Array[String]) = {

    val sparkConf = new SparkConf().setAppName("WikiPediaReceiver").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    val rootLogger = Logger.getRootLogger
    rootLogger.setLevel(Level.ERROR)

    // Create spark context with window of 10 seconds
    val ssc = new StreamingContext(sc, Seconds(10))

    try {

      val pattern = "\\[\\[(.*)\\]\\]\\s(.*)\\s(.*)\\s\\*\\s(.*)\\s\\*\\s\\(\\+?(.\\d*)\\)\\s(.*)".r
      val topics = "my-topic"

      // Spark Kafka connector requires set of topics as parameter.
      val topicsSet = topics.split(",").toSet

      // Kafka Broker to connect to read Wikipedia messages.
      val kafkaParams = Map[String, String]("metadata.broker.list" -> "localhost:9092")

      // Connects Spark Streaming to Kafka Topic and gets DStream of RDD(Wikipedia message)
      val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
      val sqlContext = new SQLContext(sc)

      // This is used to implicitly convert an RDD to a DataFrame.
      import sqlContext.implicits._

      // Uncomment following line (and comment out the next line) to shows last 30 seconds edits every 10 seconds.
      // val lines = messages.window(Seconds(30), Seconds(10)).foreachRDD{ rdd =>
      val lines = messages.foreachRDD { rdd =>

        // Create DataFrame by extracting fields from the Wikipedia message and removing unwanted rows.
        val linesDF = rdd.map(row => row._2 match {

          case pattern(title, flags, diffUrl, user, byteDiff, summary) => WikiEdit(title, flags, diffUrl, user, byteDiff.toInt, summary)
          case _ => WikiEdit("title", "flags", "diffUrl", "user", 0, "summary")
        }).filter(row => row.title != "title").toDF()

        // Number of records in 10 second window.
        val totalCnt = linesDF.count()

        // Number of bot edited records in 10 second window.
        val botEditCnt = linesDF.filter("flags like '%B%'").count()

        // Number of human edited records in 10 second window.
        val humanEditCnt = linesDF.filter("flags not like '%B%'").count()

        // % of bot edited records in 10 second window.
        val botEditPct = if (totalCnt > 0) 100 * botEditCnt / totalCnt else 0

        // % of human edited records in 10 second window.
        val humanEditPct = if (totalCnt > 0) 100 * humanEditCnt / totalCnt else 0

        println("%10s %8s %12s %10s".format("BotCount", "BotPct", "HumanCount", "HumanPct"))
        println("%10s %8s %12s %10s".format(botEditCnt, botEditPct, humanEditCnt, humanEditPct))
        println()

      }

      ssc.start()
      ssc.awaitTermination()

    } catch {
      case e: Exception => println(e.getMessage)
    }
  }
}