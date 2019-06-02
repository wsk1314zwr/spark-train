package com.wsk.spark.sql.dataset

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{LongType, StringType}

object DataSetTest {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").appName("DataSetApp").getOrCreate()


    //使用DF的API
    val df = spark.read.format("JSON").load("exampleData/people.json")

    spark.read.load()
    df.select("name").show()

    //使用DS的API
    import spark.implicits._
    val ds = spark.read.format("JSON").load("exampleData/people.json").as[People]
    ds.select("name").show()  //这种方式和DF完全一致的
    ds.map(_.name).show()   //这才是ds的正确使用方式，分析在编译时就进行检查了
  }

  case class People(name: String, age: String)

}
