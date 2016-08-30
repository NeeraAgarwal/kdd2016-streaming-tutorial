/*
 * Copyright 2016 and onwards Neera Agarwal.
 *
 */
package example

import java.io.InputStream
import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object AdClickEventReplay {
  def main(args: Array[String]) = {

    //kafkaProducer will read ad-stream and click-stream events from the files and write to Kafka topics
    val kProducer = getKafkaProducer
    start2Stream(kProducer)
    kProducer.close()
  }

  /*
   * Reads ad events from the file and write to Kafka topic "ad-stream-topic"
   * Reads click events from the file and write to Kafka topic "click-stream-topic"
   */
  def start2Stream(producer: KafkaProducer[String,String]) = {

    val stream1 : InputStream = getClass.getResourceAsStream("/AdStream.txt")
    val stream2 : InputStream = getClass.getResourceAsStream("/ClickStream.txt")
    val f1 = scala.io.Source.fromInputStream(stream1).getLines().toList
    val f2 = scala.io.Source.fromInputStream(stream2).getLines().toList

    var i = 0
    while(i < f1.length || i < f2.length) {
      if (i < f1.length) {
        val adLine = f1(i)
        producer.send(new ProducerRecord[String, String]("ad-stream-topic","ad-stream-key", adLine))
        println("ad-stream-topic:    " + adLine)
        Thread.sleep(100)
      }
      if (i < f2.length) {
        val clickLine = f2(i)
        producer.send(new ProducerRecord[String, String]("click-stream-topic","click-stream-key", clickLine))
        println("click-stream-topic: " + clickLine)
        Thread.sleep(100)
      }
      i = i+1
    }
  }

  //Creates a Kafka Producer. Kafka Producer is used to send messages to Kafka topic.
  def getKafkaProducer: KafkaProducer[String, String] ={
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("acks", "all")
    props.put("retries", "0")
    props.put("batch.size", "16384")
    props.put("linger.ms", "1")
    props.put("buffer.memory", "33554432")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)
    producer
  }

}
