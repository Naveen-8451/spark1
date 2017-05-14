package org.test.spark1

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.sql.SQLContext


object csvToparquet extends App{
  
  val conf = new SparkConf().setAppName("csvToparquet").setMaster("local[*]").set("spark.executor.memory", "1g").set("spark.driver.memory","1g")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)
  import sqlContext.implicits._
  
 // val csvDF = sqlContext.read.format("com.databricks.spark.csv").option("header", true).option("inferschema", true).load("/Users/NaveenKumar/workspace/spark1/src/main/scala/org/test/spark1/911.csv")
  //csvDF.printSchema()
  //csvDF.repartition(1)
  //val csvParquet = csvDF.write.format("parquet").save("output_parquet")
  
  
  
  val readParquet = sqlContext.read.parquet(
      "/Users/NaveenKumar/workspace/spark1/output_parquet/part-r-00000-85ffd3ba-59eb-4700-8c5f-187213759472.snappy.parquet",
      "/Users/NaveenKumar/workspace/spark1/output_parquet/part-r-00001-85ffd3ba-59eb-4700-8c5f-187213759472.snappy.parquet",
      "/Users/NaveenKumar/workspace/spark1/output_parquet/part-r-00002-85ffd3ba-59eb-4700-8c5f-187213759472.snappy.parquet",
      "/Users/NaveenKumar/workspace/spark1/output_parquet/part-r-00003-85ffd3ba-59eb-4700-8c5f-187213759472.snappy.parquet")
      readParquet.printSchema()
      val tableRDD = readParquet.registerTempTable("csvToparquet")
      sqlContext.sql("select lat from csvToparquet ").show()
     
      
      
  
 
 
  
  
}