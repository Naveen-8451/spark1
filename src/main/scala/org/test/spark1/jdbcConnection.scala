package org.test.spark1

import java.sql.DriverManager





import java.sql.Connection

object jdbcConnection {

  def main(args: Array[String]) {
    // connect to the database named "mysql" on the localhost
    
    var database = readLine("*********Enter the database**********\n")
    if(database.isEmpty()) 
    {
      val driver = "com.mysql.jdbc.Driver"
      
      //var database = readLine("*********Enter the database**********\n")
      val url = "jdbc:mysql://localhost/"+database;
      
      val username = "scott"
      val password = "tiger"
      var connection:Connection = null
      
      try {
        
      Class.forName(driver)
      connection = DriverManager.getConnection(url, username,password)
      val statement = connection.createStatement()
      database = readLine("create database:\n")
      val resultSet1 = statement.execute(database)
      
   

    // there's probably a better way to do this
      // create the statement, and run the select query
      
      while(true){
      val query = readLine("enter the query to execute:\n")
      val resultSet1 = statement.execute(query)
      println(resultSet1)
      if(resultSet1.!=(0)) println("table created")
      
      }
      
    }
      catch
      {
      case e => e.printStackTrace
      }
      connection.close()
      
    }
   
    
    else
    {
      //database = "default1"
      val driver = "com.mysql.jdbc.Driver"
      val url = "jdbc:mysql://localhost/"+database;
      val username = "scott"
      val password = "tiger"
   

    // there's probably a better way to do this
    var connection:Connection = null
    println(connection)

    try {
      // make the connection
      Class.forName(driver)
      connection = DriverManager.getConnection(url, username,password)

      // create the statement, and run the select query
      val statement = connection.createStatement()
      while(true){
      val query = readLine("enter the query to execute:\n")
      val resultSet1 = statement.execute(query)
      println(resultSet1)
      if(resultSet1.!=(0)) println("Query Executed")
      
      }
      
    } catch {
      case e => e.printStackTrace
    }
    connection.close()
      //var database = readLine("*********Enter the database**********\n")
      //database = "default1"
      // make the connection
      
    }
  }

}