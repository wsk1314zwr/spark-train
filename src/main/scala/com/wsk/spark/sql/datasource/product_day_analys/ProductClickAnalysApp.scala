package com.wsk.spark.sql.datasource.product_day_analys

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, _}
import scalikejdbc.{DB, SQL}
import scalikejdbc.config.DBs

import scala.util.parsing.json.JSON

/**
  * 产品点击日志日分析作业
  *
  * 1）产品、地址信息表存放mysql：
  * 注意：需要添加mysql的依赖
  * 2) 日点击信息表存放hdfs，为hive表，
  * 注意：1、需要添加spark_hive的依赖
  * 2、需设添加hive-site.xml到资源目录
  * 3、必须设置hadoop的安装目录不然会报空指针异常
  * 4、代码必须设置支持hive。不然不会读取hive-site.xml
  *
  */
object ProductClickAnalysApp extends Logging {

  val URL = "jdbc:mysql://192.168.175.135/test?characterEncoding=utf-8"
  val DRIVER_CLASS = ""
  val USER = "root"
  val PASSWORD = "123456"

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\appanzhuang\\cdh\\hadoop-2.6.0-cdh5.7.0")

    val spark = SparkSession.builder()
      .appName("Proudct Analys APP")
      .master("local[4]")
      .enableHiveSupport()
      .getOrCreate()
    val cityInfoDF = spark.read
      .format("jdbc")
      .option("url", URL)
      .option("user", USER)
      .option("password", PASSWORD)
      .option("query", "select city_id,city_name,area from test.city_info")
      .load()
    val productInfoDF = spark.read.format("jdbc")
      .option("url", URL)
      .option("user", USER)
      .option("password", PASSWORD)
      .option("query", "select product_id,product_name,extend_info from test.product_info")
      .load()

    val userClickDF = spark.sql("select   action_time, city_id as c_city_id, product_id as c_product_id   from wsktest.user_click")

    val getStatus = udf((jsonStr: String) => {
      var result = -1
      try {
        val json = JSON.parseFull(jsonStr) match {
          case Some(map: Map[String, Any]) => map
        }
        result = json.get("product_status").getOrElse(-1D).asInstanceOf[Double].toInt
      } catch {
        case e: Exception => logError("解析json数据错误,数据：" + jsonStr)
      }
      result
    })

    val click_product_city_baseDF = userClickDF.join(cityInfoDF, userClickDF("c_city_id") === cityInfoDF("city_id"))
      .select(col("c_product_id"), col("c_city_id"), col("city_name"), col("area"))

    val area_product_click_count = click_product_city_baseDF.groupBy(col("area"), col("c_product_id"))
      .count().withColumnRenamed("count", "click_count")

    val area_product_city_count_info = area_product_click_count.join(productInfoDF, col("c_product_id") === col("product_id"))
      .select(col("product_id"), col("product_name"), getStatus(col("extend_info")).as("product_status"),
        col("area"), col("click_count")).withColumn("day", lit("2016-05-05"))

    val area_product_city_count_rank3_info = area_product_city_count_info.select(col("product_id"), col("product_name"), col("product_status"),
      col("area"), col("click_count"), col("day")
      , row_number().over(Window.partitionBy(col("area")).orderBy(col("click_count").desc)).as("rank"))
      .where(col("rank") <= 3).coalesce(1)

//
//    area_product_city_count_rank3_info.foreachPartition(partition => {
//      DBs.setupAll()
//      DB.localTx { implicit session =>
//        while (partition.hasNext) {
//          val row = partition.next()
//          SQL("insert into product_area_click_rank values(?,?,?,?,?,?,?)")
//                  .bind(row.getInt(0), row.getString(1), row.getInt(2), row.getString(3), row.getLong(4), row.getString(5), row.getInt(6))
//                  .update().apply()
//
//        }
//      }
//      DBs.closeAll()
//    })
    spark.stop()

  }
}

