package org.test.spark1

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel._
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.{StructType,StructField,StringType};

object sqlSpark {
  def main(args:Array[String]){          
  val conf = new SparkConf().setAppName("DataFrame").setMaster("local")
            val sc = new SparkContext(conf)
          
            /***************************************/
            //sqlContext Object creation 
            /***************************************/
    
            val sqlContext = new SQLContext(sc)
            import sqlContext.implicits._
            val schemaString = "id type"
            val column_name = StructType(schemaString.split(",").map(fieldName => StructField(fieldName, StringType, true)))
            val file = sqlContext.read.format("text").schema(column_name)load("post.txt")
            //sqlContext.read.format("text").schema(column_name).write.save("post.txt")
            file.registerTempTable("post")
            sqlContext.sql("show tables").show
            sqlContext.sql("select * from post").show
  }  
}