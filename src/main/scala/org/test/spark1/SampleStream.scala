package org.test.spark1

import org.apache.spark.streaming.flume._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._ 
import org.apache.spark.SparkConf
import org.apache.flume.auth.FlumeAuthenticationUtil
import org.apache.spark.SparkContext

object SampleStream {
def main(args: Array[String]) {
  val conf = new SparkConf().setMaster("local[2]").setAppName("SampleStream")
  val ssc = new StreamingContext(conf, Seconds(1))
//  val flumeStream = FlumeUtils.createStream(ssc, "localhost", 1234)
//  flumeStream.
  val lines = ssc.socketTextStream("localhost", 9999)
  val words = lines.flatMap(_.split(" "))
  val pairs = words.map(word => (word, 1))
  val wordCounts = pairs.reduceByKey(_ + _)
  wordCounts.print()
  ssc.start()             // Start the computation
  ssc.awaitTermination()  
  ssc.stop()
  
  }
}
