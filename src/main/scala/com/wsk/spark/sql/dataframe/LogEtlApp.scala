package com.wsk.spark.sql.dataframe

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


/**
  * 使用 DataFrame 进行ETL作业
  *
  * 1、Spark session 是一切操作的入口
  */
object LogEtlApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("ETL APP")
      .master("local[2]")
      .getOrCreate()

    import spark.implicits._

    val logDF = spark.sparkContext.textFile("data/df_log_analys/input/hadoop-click-log.txt",1)
      .map(_.split("\t"))
      .filter(_.length == 72)
      .map(splits => LogInfo(splits(0)
        , splits(1)
        , splits(3)
        , splits(4).substring(1, splits(4).length() - 7)
        , splits(6)
        , splits(10)
        , splits(12)
        , splits(20).toLong))
      .toDF()

    logDF.printSchema()

    logDF.filter(  $"traffic">0)
      .rdd
      .saveAsTextFile("data/df_log_analys/output/d=20190521")

    spark.stop()
  }

  case class LogInfo(cdn: String, region: String, level: String,
                     tiem: String, ip: String, domain: String,
                     url: String, traffic: Long)

}
