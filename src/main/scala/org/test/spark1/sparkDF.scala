package org.test.spark1


import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel._


object sparkDF {
    case class Marks(Id:String,Course_Id:String,Course_Name:String,Internal_Marks:Int,External_Marks:Int,Total:Int,Credit:Int)
    def main(args:Array[String])
    {
            val conf = new SparkConf().setAppName("DataFrame").setMaster("local")
            val sc = new SparkContext(conf)
          
            /***************************************/
            //sqlContext Object creation 
            /***************************************/
    
            val sqlContext = new SQLContext(sc)
            import sqlContext.implicits._
           
            
            val path = readLine("enter the path:\n")
            val MarksRDD = sc.textFile(path)
            println(MarksRDD.first())
            
            MarksRDD.persist(DISK_ONLY)
            
            val MarksRDD1 = MarksRDD.map(x=>x.split(","))
            .map(p=>Marks(p(0),p(1),p(2),p(3).toInt,p(4).toInt,p(5).toInt,p(6).toInt)).toDF()
            
            println(MarksRDD1.take(3))
            
            MarksRDD1.registerTempTable("Marks")
            
            val resultRDD = sqlContext.sql("show tables").show
            val queryRDD = sqlContext.sql("select * from  Marks where Course_Name = 'COMPUTER PROGRAMMING'" ).show
            val CreateRDD = sqlContext.sql("with fails as (select * from Marks where (Credits = 0))").show
    }
}