package org.test.spark1

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SQLContext._
//import org.eclipse.jetty.http.HttpFields.Field

object schemaDefine extends App{
  
  //Setting SpatkConf.
  
  val conf = new SparkConf().setAppName("schema").setMaster("local")
  
  //Setting Spark Context.
  
  val sc = new SparkContext(conf)
  
  //Setting SparkSQL Context.
  
  val sqlContext = new SQLContext(sc)
  
  //To convert Implicitly from RDD to DataFrame we use this Import.
  
  import sqlContext.implicits._
  
  //Creating an RDD using Existing Data sc.textFile("path")
  
  val fileRDD = sc.textFile("table")
  
  //we encode the schema before defining it. 
  
  val schemaRDD = "Account Date Status Amount"
  
  //To define the Schema from the encoded string we have to Import the ROW.
  
  import org.apache.spark.sql.Row
  
  //To define the Schema from encoded String we have to define SparkSQL DataTypes
  
  import org.apache.spark.sql.types.{StructType,StructField,StringType}
  
  //We have imported all libraries now we define schema with out Case Class using sqlTypes.
  
  val SchemaType = StructType(schemaRDD.split(" ").map(fieldName => StructField(fieldName, StringType, true)))
  
  // Convert records of the RDD (people) to Rows.
  
  val rowRDD = fileRDD.map(_.split("\t")).map(p => Row(p(0).toInt, p(1), p(2), p(3).toDouble))
  
  // Apply the schema to the RDD.
  
  val fileSchemaRDD = sqlContext.createDataFrame(rowRDD, SchemaType)
  println(fileSchemaRDD)
  //Register a TempTable.
  
  val a  = fileSchemaRDD.createTempView("Accounts")
  println(a)
  
  val results = sqlContext.sql("SELECT Date FROM Accounts")
  results.show()
  
  //Save to Hadoop Location .
  
  results.write.format("json").save("hdfs://localhost:8080/user/Accounts_Data7")

  
  
  
  
}