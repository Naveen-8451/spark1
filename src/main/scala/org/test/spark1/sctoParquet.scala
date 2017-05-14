package org.test.spark1


import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.sql.SQLContext


object sctoParquet extends App {
  
  
  val conf = new SparkConf().setAppName("csvToparquet").setMaster("local[*]").set("spark.executor.memory", "1g").set("spark.driver.memory","1g")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)
  import sqlContext.implicits._
  
  val textRDD = sc.textFile("/Users/NaveenKumar/workspace/spark1/output_parquet/part-r-00000-85ffd3ba-59eb-4700-8c5f-187213759472.snappy.parquet")
  println(textRDD.first())
  val parquet = sqlContext.read.parquet("/Users/NaveenKumar/workspace/spark1/output_parquet/part-r-00000-85ffd3ba-59eb-4700-8c5f-187213759472.snappy.parquet")
  println(parquet.printSchema())
  
}