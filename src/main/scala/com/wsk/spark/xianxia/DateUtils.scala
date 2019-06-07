package com.wsk.spark.xianxia

import java.util.{Date, Locale}

import org.apache.commons.lang3.time.FastDateFormat
import org.apache.spark.internal.Logging

/**
  * Author: Michael PK   QQ: 1990218038
  * 日期时间的工具类
  */
object DateUtils extends Logging {

  //  /**
  //    * 10	name10	2019-06-08 10:40:39	8332
  //    *
  //    * /..../2019060810
  //    * ==>
  //    * day=20190608/hour=10
  //    */
  //FastDateFormat是安全的，simpledataFormate的线程不安全，会完犊子
  val SOURCE_TIME_FORMAT: FastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss", Locale.ENGLISH)

  val TARGET_TIME_FORMAT: FastDateFormat = FastDateFormat.getInstance("yyyyMMddHHmmss")

  // 获取时间戳
  def getTime(time: String): Long = {
    try {
      SOURCE_TIME_FORMAT.parse(time).getTime
    } catch {
      case e: Exception =>
        logError(s"$time parse error: ${e.getMessage}")
        0l
    }
  }

  def parseToMinute(time: String): String = {
    TARGET_TIME_FORMAT.format(new Date(getTime(time)))
  }

  def main(args: Array[String]): Unit = {
    val tmp = parseToMinute("2019-04-05 10:14:09")
    println(getDay(tmp))
    println(getHour(tmp))
  }

  def getDay(minute: String): String = {
    minute.substring(0, 8)
  }

  def getHour(minute: String): String = {
    minute.substring(8, 10)
  }
}
