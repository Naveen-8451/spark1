package org.test.spark1

import org.apache.spark.SparkConf

import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel._
import org.apache.spark.sql.hive.HiveContext

object fileread {
  def main(args:Array[String])
    {
            val conf = new SparkConf().setAppName("DataFrame").setMaster("local")
            val sc = new SparkContext(conf)
          
            /***************************************/
            //sqlContext Object creation 
            /***************************************/
    
            val sqlContext = new SQLContext(sc)
            import sqlContext.implicits._
            val hiveContext = new HiveContext(sc)
            import hiveContext.implicits._
            val file = sqlContext.read.format("com.databricks.spark.csv").load("hdfs://localhost:8080/user/yahoo_stocks.csv")
            val header = file.first
            //val data = file.
            file.registerTempTable("genre")
            file.sqlContext.sql("select * from genre").show
            file.write.format("com.databricks.spark.csv").save("hdfs://localhost:8080/yahoo")
           

    }
  
}