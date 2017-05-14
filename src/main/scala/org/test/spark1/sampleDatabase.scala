package org.test.spark1

import org.apache.spark.SparkConf 

import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel._
import org.apache.spark.sql.hive.HiveContext
import java.sql.DriverManager
import java.sql.Connection
//import org.apache.spark.sql.HiveContext

//import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.{StructType,StructField,StringType};

object sampleDatabase {
  def main(args:Array[String])
    {
            val conf = new SparkConf().setAppName("DataFrame").setMaster("local")
            val sc = new SparkContext(conf)
          
            /***************************************/
            //sqlContext Object creation 
            /***************************************/
      
            val sqlContext = new HiveContext(sc)
            import sqlContext.implicits._
            
            /**************************************/
            //Connection to JDBC
            /**************************************/
            val driver = "com.mysql.jdbc.Driver"
            val url ="jdbc:mysql://localhost/sample2"
            val username = "scott"
            val password = "tiger"
            val tableName = readLine("enter table you wanted to Pull:\n")
            val path = readLine("enter HDFS Path to save\n")
            var connection:Connection = null
            try {
            // make the connection
            Class.forName(driver)
            connection = DriverManager.getConnection(url, username,password)
            
            val dataframe_mysql = sqlContext.read.format("jdbc")
            .option("url", url)
            .option("driver", driver)
            .option("dbtable", tableName).option("user", username)
            .option("password", password).load()
             println(dataframe_mysql.show)
            dataframe_mysql.registerTempTable(tableName)
            //dataframe_mysql.createJDBCTable(url, tableName, true)
           // dataframe_mysql.insertIntoJDBC(url, tableName, true)
            val query = readLine("enter the Query to execute:\n")
            val resultSet = dataframe_mysql.sqlContext.sql(query)
            
            resultSet.collect.foreach(println)
            resultSet.write.format("com.databricks.spark.csv").save(path+"/"+tableName)
                       //occupation
            //hdfs://localhost:8080/genre_incremental
             //select * from genre where lastupdated>'2016-08-14 05:22:00';
            }
            catch {
              case e => e.printStackTrace
            }
            
  
  }
}