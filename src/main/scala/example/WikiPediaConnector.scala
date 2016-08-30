/*
 * Copyright 2016 and onwards Neera Agarwal.
 *
 */
package example
import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.schwering.irc.lib.{IRCConnection, IRCEventListener, IRCModeParser, IRCUser}


object WikiPediaConnector {

  /*
   * Connect to Wikipedia IRC channel and write Wikipedia edit events to Kafka topic.
   */
  def main(args: Array[String]) = {

    // create Kafka producer
    val kProducer = getKafkaProducer

    // create connection object for Wikipedia IRC
    val conn = getIrcConnection(kProducer)

    try {
      conn.connect()
      // listen to Wikipedia english language IRC channel
      conn.send("JOIN " + "#en.wikipedia")
      conn.run()
    }
    finally {
      if (conn.isConnected)
        conn.close()
    }
  }

  /*
   * Registers Wikipedia listener object and passes Kafka producer to it. Returns the connection to Wikipedia IRC edit stream.
   */
  def getIrcConnection(producer: KafkaProducer[String,String]) = {
    val host = "irc.wikimedia.org"                              // default host for Wikipedia IRC
    val port = 6667                                             // default port for Wikipedia IRC
    val nick = "KDD-Tutorial-" +  (Math.random() * 1000).toInt
    val conn = new IRCConnection(host, Array(port), "", nick, nick, nick)
    conn.addIRCEventListener(new WikipediaIrcChannelListener(producer))
    conn.setEncoding("UTF-8")
    conn.setPong(true)
    conn.setColors(false)
    conn.setDaemon(true)
    conn.setName("WikipediaEditEventIrcStreamThread")
    conn
  }

  /*
   * Creates a Kafka Producer. Kafka Producer is used to send messages (wiki edit events) to Kafka topic.
   */
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

/*
 * This class receives to Wikipedia IRC edit events and writes them to Kafka topic.
 */
class WikipediaIrcChannelListener( producer: KafkaProducer[String, String]) extends IRCEventListener {

  def onPrivmsg(target:String, user: IRCUser , msg: String) {
    println(msg)
    producer.send(new ProducerRecord[String, String]("my-topic",msg.substring(0,10), msg))
  }

  def onRegistered() = print("Connected.")
  def onError(s:String) = print(s)
  def onError(i: Int, s:String) = print(s)
  def onNick(user: IRCUser, s:String) = print(s)
  def onQuit(user: IRCUser, s:String) = print(s)
  def onPart(s1: String, user: IRCUser, s:String) = print(s)
  def onMode(s: String, user: IRCUser, ircModeParser:IRCModeParser) = print(s)
  def onMode( user: IRCUser, s:String, s1:String) = print(s)
  def onTopic(s1: String, user: IRCUser, s:String) = print(s)
  def onPing(s:String) = print(s)
  def onReply(i: Int, s:String, s1:String) = print(s)
  def onInvite(s1: String, user: IRCUser, s:String) = print(s)
  def onKick(s: String, user: IRCUser, s1:String, s2:String) = print(s)
  def unknown(s: String, s1:String, s2:String, s3:String) = print(s)
  def onNotice(s: String, user: IRCUser, s1:String) = {}
  def onJoin(s: String, user: IRCUser) = print(s)
  def onDisconnected() = print("Disconnected")
}
