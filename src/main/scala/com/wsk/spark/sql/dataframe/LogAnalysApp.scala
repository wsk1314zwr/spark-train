package com.wsk.spark.sql.dataframe

import com.wsk.spark.sql.dataframe.LogEtlApp.LogInfo
import org.apache.spark.sql.SparkSession

/**
  * 使用 DataFrame 进行Analys
  *
  * 1、Spark session 是一切操作的入口
  *
  * 需求一：求每个域名访问量前10的URL
  *
  *
  */
object LogAnalysApp {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Analys APP")
      .master("local[2]")
      .getOrCreate()

    import spark.implicits._

    val logDF = spark.sparkContext.textFile("data/cdnlog/input/CDN_click.log")
      .map(_.split(","))
      .filter(_.length == 8)
      .map(splits => LogInfo(splits(0)
        , splits(1)
        , splits(2)
        , splits(3)
        , splits(4)
        , splits(5)
        , splits(6)
        , splits(7).toLong)
      )
      .toDF()

    logDF.printSchema()
    println("加载数据：" + logDF.count())
    logDF.sort($"name".desc,$"name".asc)
    logDF.join(logDF,logDF("id")===logDF("id"))
    logDF.select()createOrReplaceTempView("log_info_temp")
    spark.sql("select domain,url,count(1) as num from log_info_temp group by domain,url ")
      .createOrReplaceTempView("domain_url_count_info_temp")

    spark.sql("select t.* ,\'2019-05-21\' as day from ( " +
      "select domain ,url, num,row_number() over(partition by domain order by num desc) rank " +
      "from domain_url_count_info_temp) t " +
      "where t.rank <=10").show()
//      rdd.coalesce(1).saveAsTextFile("data/df_log_analys/output2/d=2019-05-21")


    spark.stop()
  }

  case class LogInfo(cdn: String, region: String, level: String,
                     tiem: String, ip: String, domain: String,
                     url: String, traffic: Long)

}
