package com.wsk.spark.sql.datasource.customdatasource

import org.apache.spark.sql.SparkSession

/**
  * 自定义外部数据源，实现读取非内置的内置类型的数据读取为DF。
  */

object Main {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("customds")
      .getOrCreate()

   val textDF =  spark.read.format("text").load("exampleData/kv1.txt")
//   内置的text的schema为v只有一个字段value 。我们除了通过case class方式生成有schema DF外，
//    还可以自定义数据转换器
//     root
//    |-- value: string (nullable = true)


    //自定义的自测
     val wskDF = spark.read.format("com.ruoze.spark.sql.customdatasource.TextRelationProvider").option("path","exampleData/wsk_employer.txt").load()
     wskDF.printSchema()
    wskDF.show()


    spark.stop()
  }
}
