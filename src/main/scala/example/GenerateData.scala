/*
 * Copyright 2016 and onwards Neera Agarwal.
 *
 * Generate data files for Ad events and click events streams.
 * 1. Ad Stream: QueryID (100 ids), Advt Id, timestamp
 * 2. Click Stream: QueryId, ClickId, timestamp
 *  Time stamp is Unix time - Long
 *  Click events will occur after Ad events. Click can occur in any order.
 */

package example

import java.io._

import scala.collection.mutable.ListBuffer

object GenerateData {

  def main(args: Array[String]) {
    generateAdStreamData()
    generateClickStreamData()
  }

  def generateAdStreamData() = {

    val fw = new PrintWriter(new File("/Users/<username>/kdd2016-streaming-tutorial/src/main/resources/AdStream.txt" ))
    val qId = scala.util.Random
    val adId = scala.util.Random
    for (i <- 1 to 1000) {
      val timestamp: Long = System.currentTimeMillis
      val s = qId.nextInt(10000) + ", " + adId.nextInt(10)+ ", " +  timestamp + "\n"
      fw.write(s)
    }
    fw.close()
  }

  def generateClickStreamData() = {

    val fw = new PrintWriter(new File("/Users/<username>/kdd2016-streaming-tutorial/src/main/resources/ClickStream.txt"))
    val fo = scala.io.Source.fromFile("/Users/<username>/kdd2016-streaming-tutorial/src/main/resources/AdStream.txt")

    var idList: ListBuffer[Int] = new ListBuffer[Int]()
    for (i <- fo.getLines()) {
      println(i)
      idList += i.split(", ")(0).toInt
    }
    val clickId = scala.util.Random
    for (i <- 1 to 1000) {
      val timestamp: Long = System.currentTimeMillis
      val s = idList.toList(i-1) + ", " + clickId.nextInt(100000)+ ", " +  timestamp + "\n"

      fw.write(s)
    }
    fo.close()
    fw.close()
  }
}
