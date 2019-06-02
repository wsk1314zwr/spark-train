package com.wsk.spark.jdbc

import java.sql.DriverManager

/**
  * jdbc步骤：
  * 1)加载相应的DBS驱动类
  * 2)获取connection
  * 3)获取statement
  * 4)获取result结果
  * 5)关闭链接
  *
  */

object SimpleJDBC {
  def main(args: Array[String]): Unit = {

//    Class.forName("com.mysql.jdbc.Driver")
    classOf[com.mysql.jdbc.Driver]
    val url = "jdbc:mysql://192.168.175.135/test?characterEncoding=utf-8"
    val user = "root"
    val password = "123456"
    val connection = DriverManager.getConnection(url, user, password)
    val stmt = connection.createStatement()

    val sql= " select name,age,salary from wsktest;"
    val resultSet = stmt.executeQuery(sql)
    while(resultSet.next()){
      val name = resultSet.getString(1)
      val age = resultSet.getInt(2)
      val salary = resultSet.getLong(3)
      println(name+":"+age+":"+salary)
    }

    resultSet.close()
    stmt.close()
    connection.close()
  }
}
