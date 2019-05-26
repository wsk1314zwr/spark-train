package com.wsk.spark.sql.textdatasource

import org.apache.spark.sql.types.{DataType, LongType, StringType}

/**
  * 转换类型
  */
object Utils {

  def castTo(value:String, dataType:DataType) ={
    dataType match {
      case _:LongType => value.toLong
      case _:StringType => value
    }
  }

}
