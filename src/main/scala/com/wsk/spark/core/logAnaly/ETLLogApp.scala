package com.wsk.spark.core.logAnaly

import java.text.SimpleDateFormat
import java.util.Locale

import org.apache.commons.lang3.StringUtils
import org.apache.spark.{SparkConf, SparkContext}

/**
  * ETL
  * 使用spark core进行离线的每日点击日志数据清洗工作
  */

object ETLLogApp {
  //TODO 时间转换用java的方式失败，待改进
  //  val sourceFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss", Locale.ENGLISH)
  //  val targetFormat = new SimpleDateFormat("yyyyMMddHHmmss")

  def main(args: Array[String]): Unit = {

    if(args.length != 2) {
      System.err.println("please input 2 params: input output")
      System.exit(0)
    }

//    val conf = new SparkConf().setAppName("CDN-log-Analysis").setMaster("local[2]")
//    val sc = new SparkContext(conf)
    val sc = new SparkContext()

    val lines = sc.textFile(args(0), 1)
    println("共加载数据：" + lines.count())
    lines.map(parse(_)).saveAsTextFile(args(1))

    sc.stop()
  }


  def parse(log: String): String = {
    val splits = log.split("\t")
    val cdn = splits(0)
    val region = splits(1)
    val level = splits(3)
    val timeStr = splits(4)
    var time = timeStr.substring(1, timeStr.length() - 7)
    val ip = splits(6)
    val domain = splits(10)
    val url = splits(12)
    val traffic = splits(20)

    new StringBuilder("")
      .append(cdn).append(",")
      .append(region).append(",")
      .append(level).append(",")
      .append(time).append(",")
      .append(ip).append(",")
      .append(domain).append(",")
      .append(url).append(",")
      .append(traffic).toString()

  }

}
