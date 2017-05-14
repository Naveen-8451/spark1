package org.test.spark1

object hospital extends App{
  
   val session = org.apache.spark.sql.SparkSession.builder.master("local").appName("Spark CSV Reader").getOrCreate;
   val df = session.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("/Users/NaveenKumar/workspace/spark1/src/main/scala/org/test/spark1/table.csv")
   df.show
   df.printSchema()
//   df.registerTempTable("hospital_charges")
//   df.groupBy("ProviderState").avg("AverageCoveredCharges").show
//   df.groupBy("ProviderState").avg("AverageTotalPayments").show
//   df.groupBy("ProviderState").avg("AverageMedicarePayments").show
//   df.groupBy(("ProviderState"),("DRGDefinition")).sum("TotalDischarges").show
   
}