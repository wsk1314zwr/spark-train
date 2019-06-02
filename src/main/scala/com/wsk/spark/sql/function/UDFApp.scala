package com.wsk.spark.sql.function

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * Spark sql Function
  * 1）自带function测试
  *
  * 2）自定义函数:求喜欢的人个数
  */
object UDFApp {


  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder().master("local[2]").appName("FunctionApp").getOrCreate()

    /**
      * 内置的函数
      */
    //    val rdd = spark.sparkContext.textFile("data/etlLog/input/hadoop-click-log.txt")
    import spark.implicits._
//
//
//
//    val df = rdd.map(x => {
//      val fileds = x.split("\t")
//      if (fileds.length == 72) {
//        (fileds(10), 1L)
//      } else {
//        ("-", 1L)
//      }
//    }).toDF("domain", "times")
//
//    //使用count函数，注意必须必须受手动的导入functions的包
//    df.groupBy("domain").agg(count("times").as("pv"))
//      .select("domain", "pv")
//      .show()

    /**
      * 自定义函数
      */
    val likeDF= spark.sparkContext.parallelize(List("17er\truoze,j哥,星星,小海", "老二\tzwr,17er", "小海\t苍老师,波老师"))
      .map(x => {
        val fileds = x.split("\t")
        Stu(fileds(0).trim, fileds(1).trim)
      }
      ).toDF()

    //方式一：通过register()方法定义，sql方式使用
    spark.udf.register("like_count" ,(like:String)=>like.split(",").size)
    likeDF.createOrReplaceTempView("info")
    spark.sql("select name,like,like_count(like) num from info").show()

    //方式二：通过udf方法定义，DF的API方式使用
    val like_count2 = udf((like:String)=>like.split(",").size)
    likeDF.select(col("name"),col("like"),like_count2(col("like"))).show()

    /**
      * 使用Catalog操作SQL
      */

    val catalog = spark.catalog
    catalog.listTables().show()
    catalog.listFunctions().show()

    spark.stop()
  }
  case class Stu(name: String, like: String)
}

