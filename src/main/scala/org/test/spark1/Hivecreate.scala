package org.test.spark1

import org.apache.spark.SparkConf

import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.{StructType,StructField,StringType};
object Hivecreate {
  def main(args:Array[String])
    {
            val conf = new SparkConf().setAppName("DataFrame").setMaster("local")
            val sc = new SparkContext(conf)
          
            /***************************************/
            //sqlContext Object creation 
            /***************************************/
    
            //val sqlContext = new SQLContext(sc)
            //import sqlContext.implicits._
            val hiveContext = new HiveContext(sc)
            import hiveContext.implicits._
            hiveContext.sql("CREATE TABLE IF NOT EXISTS employee(id INT, name STRING, age INT)ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'")
            hiveContext.sql("LOAD DATA INPATH 'hdfs://localhost:8080/employee.txt' INTO TABLE employee")
            val result = hiveContext.sql("FROM employee SELECT id, name, age")
            result.show
    }
  
}