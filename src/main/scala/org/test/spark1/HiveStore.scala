package org.test.spark1

import org.apache.spark.sql.hive.orc._
import org.apache.spark.sql._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.DataFrame


object HiveStore
{
  case class YahooStockPrice(date: String, open: Float, high: Float, low: Float, close: Float, volume: Integer, adjClose: Float)
  def main(args:Array[String])
    {
            val conf = new SparkConf().setAppName("DataFrame").setMaster("local")
            val sc = new SparkContext(conf)
          
            /***************************************/
            //sqlContext Object creation 
            /***************************************/
    
            
            val hiveContext = new HiveContext(sc)
            import hiveContext.implicits._
            
            val HiveCreate = hiveContext.sql("create table yahoo_orc_table (date STRING, open_price FLOAT, high_price FLOAT, low_price FLOAT, close_price FLOAT, volume INT, adj_price FLOAT) stored as orc")
            val yahoo_stocks = sc.textFile("hdfs://localhost:8080/user/yahoo_stocks.csv")
            yahoo_stocks.take(10)
            val header = yahoo_stocks.first
            val data = yahoo_stocks.filter(_ != header)
            data.first
            val stockprice = data.map(_.split(",")).map(x => YahooStockPrice(x(0),x(1).toFloat,x(2).toFloat,x(3).toFloat,x(4).toFloat,x(5).toInt,x(6).toFloat)).toDF()
            stockprice.first
            stockprice.show
            stockprice.printSchema
            stockprice.registerTempTable("yahoo_stocks_temp")
            val results = hiveContext.sql("SELECT * FROM yahoo_stocks_temp")
            results.map(t => "Stock Entry: " + t.toString).collect().foreach(println)
            results.write.format("orc").save("hdfs://localhost:8080/yahoo_stocks_orc")

    }
}