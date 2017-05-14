package org.test.spark1


import org.apache.spark._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

object emergency extends App {
  
              val conf =  new SparkConf().setAppName("Emergency").setMaster("local")
              val sc = new SparkContext(conf)
              val sqlContext = new SQLContext(sc)
              import sqlContext.implicits._
              val data = sc.textFile("/Users/NaveenKumar/workspace/spark1/src/main/scala/org/test/spark1/911.csv")
             
                val header = data.first()
                 
                val data1 = data.filter(row => row != header)
                 
                case class emergency(lat:String,lng:String,desc:String,zip:String,title:String,timeStamp:String,twp:String,addr:String,e:String)
                 
                val emergency_data = data1.map(x=>x.split(",")).filter(x => x.length>=9).map(x => emergency(x(0),x(1),x(2),x(3),x(4).substring(0 , x(4).indexOf(":")),x(5),x(6),x(7),x(8))).toDF
                 
                emergency_data.registerTempTable("emergency_911")
                
                val data2 = sc.textFile("/Users/NaveenKumar/workspace/spark1/src/main/scala/org/test/spark1/zipcode.csv")
                 
                val header1 = data2.first()
                 
                val data3 = data2.filter(row => row != header1)
                 
                case class zipcode(zip:String,city:String,state:String,latitude:String,longitude:String,timezone:String,dst:String)
                 
                val zipcodes = data3.map(x => x.split(",")).map(x=> zipcode(x(0).replace("\"", "")
                 
                ,x(1).replace("\"", ""),x(2).replace("\"", ""),x(3),x(4),x(5),x(6))).toDF
                 
                zipcodes.registerTempTable("zipcode_table")
                
                val build1 = sqlContext.sql("select e.title, z.city,z.state from emergency_911 e join zipcode_table z on e.zip = z.zip").show
               }