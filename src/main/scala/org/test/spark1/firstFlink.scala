package org.test.spark1


import org.apache.flink._
import org.apache.kafka._
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala._ 

object firstFlink {
   def main(args: Array[String]): Unit = {
   val env = StreamExecutionEnvironment.getExecutionEnvironment
   val socketStream = env.socketTextStream("localhost",9000)
  // val wordsStream = socketStream.map(value => (value,1))
  // val keyValuePair = wordsStream.keyBy(0)
  // val countPair = keyValuePair.sum(1)
   
   
  }
}