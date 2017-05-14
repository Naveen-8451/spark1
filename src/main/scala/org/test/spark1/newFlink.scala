package org.test.spark1

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala.DataSet

object newFlink {
  
   val env = ExecutionEnvironment.getExecutionEnvironment
   val lines:DataSet[String] = env.readTextFile("/Users/NaveenKumar/workspace/spark1/src/main/scala/org/test/spark1/social_friends.csv")
   //val Average = lines.map(line =>(line.split(","))).map(word => (word(2).toString,word(3).toInt,1))
   
}