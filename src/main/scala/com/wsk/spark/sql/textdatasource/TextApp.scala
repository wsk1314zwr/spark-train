package com.wsk.spark.sql.textdatasource

import org.apache.spark.sql.SparkSession

/**
  * 测试
  */
object TextApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("TextApp").master("local[2]").getOrCreate()
    val df = spark.read.format("com.wsk.spark.sql.textdatasource").option("path","D://wskspace/workspace1/data/spark-train/data/textdatasource/*").load()
    df.show()

    spark.stop()
  }

}
