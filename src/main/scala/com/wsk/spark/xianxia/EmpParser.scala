package com.wsk.spark.xianxia

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

/**
  * Author: Michael PK   QQ: 1990218038
  */
object EmpParser {
  val struct = StructType(Array(
    StructField("id", IntegerType),
    StructField("name", StringType),
    StructField("time", LongType),
    StructField("salary", DoubleType),
    StructField("day", StringType),
    StructField("hour", StringType)
  ))

  // ds/df = spark.createDataFrame(rdd, schema)

  def parseLog(log:String): Row = {
    try{
      val splits = log.split("\t")
      val id = splits(0).toInt
      val name = splits(1)
      val salary = splits(3).toDouble

      val time = DateUtils.getTime(splits(2))
      val minute = DateUtils.parseToMinute(splits(2))
      val day = DateUtils.getDay(minute)
      val hour = DateUtils.getHour(minute)

      Row(id,name,time,salary,day,hour)  // 6
    } catch {
      case e:Exception => e.printStackTrace()
        Row(0)
    }
  }
}
