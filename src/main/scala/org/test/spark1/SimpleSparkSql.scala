package org.test.spark1

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SQLContext._
import scala.io.Source

object SimpleSparkSql extends App{
  
  val conf = new SparkConf().setAppName("My_App").setMaster("local")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)
  import sqlContext.implicits._
  val filename = "table"
  val fileLines = Source.fromFile(filename).getLines.toList
  fileLines.foreach(println)
  
  val data = Seq(fileLines).toDS()
  data.show()
  //map(_.split("\t")).map(x=>data((0),x(1),x(2),x(3).toString)).toDF("Account","Date"," Type","Amount")
  //val a = data.sqlContext.createExternalTable("smaple","/Users/NaveenKumar/workspace/spark1/src/main/scala/org/test/spark1")
  
  
  
  

//  val sqlContext = new SQLContext(sc)
//  val d1 =  List(("Alberto", 2), ("Dakota", 2))
  
  
}