package org.test.spark1

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.{unix_timestamp, to_date}


case class Accounts(Account:Int,dt:String,Status:String,Amount:Int)
object TypeChnage extends App {
  
  val Conf = new SparkConf().setAppName("ChangeType").setMaster("local")
  val sc = new SparkContext(Conf)
  val sqlContext = new SQLContext(sc)
  import sqlContext.implicits._
  val textData = sc.textFile("table")
  val rDD1 = textData.map(x=>x.split(",")).map(x=>Accounts(x(0).toInt,x(1),x(2),x(3).toInt)).toDF()
 // val s = rDD1.withColumn("date", 'dt.cast("TimeStamp")).select('Account,'date as 'dt,'Status,'Amount)
  rDD1.printSchema()
  val table = rDD1.registerTempTable("accounts")
  sqlContext.sql("Desc accounts").show()
  sqlContext.sql("select * from accounts").show()
  
}