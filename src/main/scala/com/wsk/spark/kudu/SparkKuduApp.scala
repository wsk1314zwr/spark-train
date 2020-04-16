package com.wsk.spark.kudu

import java.util.Properties

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * 讲师：若泽(PK哥)
  * 交流群：126181630
  */
object SparkKuduApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local").getOrCreate()
    val KUDU_MASTERS = "hadoop000"

    val properties = new Properties()
    properties.put("user","root")
    properties.put("password","root")

//    val jdbcDF = spark.read
//      .jdbc("jdbc:mysql://hadoop000:3306/spark","wc",properties)
//      .filter("cnt > 11")
//
//    jdbcDF.write
//        .mode(SaveMode.Append)
//      .format("org.apache.kudu.spark.kudu")
//      .option("kudu.master", KUDU_MASTERS)
//      .option("kudu.table","g6_pk")
//      .save()

    import spark.implicits._
    spark.read.format("org.apache.kudu.spark.kudu")
          .option("kudu.master", KUDU_MASTERS)
          .option("kudu.table","g6_pk")
      .load().sort($"cnt".desc).show()

    spark.stop()
  }

}
