package org.test.spark1

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object wordcount {
  def main(args: Array[String]): Unit = {
    
    val conf = new SparkConf().setAppName("WordCount")
    .setMaster("local")
    val sc = new SparkContext(conf)
    val word = sc.textFile("/Users/NaveenKumar/Desktop/marks3.csv")
    val RDD = word.flatMap( line =>line.split(",")).map( word1 => (word1,1)).reduceByKey(_+_)
   .saveAsTextFile("hdfs://localhost:8080/user/Ip2.txt")
  }
  
}