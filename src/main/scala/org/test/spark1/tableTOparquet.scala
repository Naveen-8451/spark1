package org.test.spark1

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.sql.SQLContext

object tableTOparquet extends App{
  
  val conf = new SparkConf().setAppName("csvToparquet").setMaster("yarn").set("spark.executor.memory", "1g").set("spark.driver.memory","1g")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)
  import sqlContext.implicits._
  val tableDF = sqlContext.read.format("com.databricks.spark.csv").option("header", true).option("inferschema", true).load("/Users/NaveenKumar/workspace/spark1/src/main/scala/org/test/spark1/table.csv")
  tableDF.printSchema()
  tableDF.coalesce(4)
  tableDF.persist()
  //val csvParquet = tableDF.write.format("parquet").save("output_table")
  
  val readParquet = sqlContext.read.parquet(
      "/Users/NaveenKumar/workspace/spark1/output_table/part-r-00000-65abb874-8466-4905-ac52-6c1198113393.snappy.parquet")
      readParquet.printSchema()
      val tableRDD = readParquet.registerTempTable("tableParquet")
      sqlContext.sql("select * from tableParquet ").show()
  
      val dataRDD = sqlContext.read.text("/Users/NaveenKumar/Desktop/Numers.txt")
      val header = dataRDD.first()
      val dataRDD1 =  dataRDD.filter(row => row != header)
      dataRDD.registerTempTable("numbers")
      sqlContext.sql("select * from numbers").show()
}