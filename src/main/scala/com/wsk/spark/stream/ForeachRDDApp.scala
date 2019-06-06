package com.wsk.spark.stream

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 1）foreachRDD是生产中用到的最多的output函数，通过它可将DStream转换为RDD以及DF\DS，然后进行操作
  * 2）foreachRDD的方法是在driver中进行的，故写DB时它的获取连接代码必须写在第二层循环的foreachPartition中，不然会报序列化错误。
  * 3）踩坑 ，在foreachPartition中数据的获取使用的是迭代器，不管是java还是Scala，迭代器只能使用一次，第二次就位空了
  * 4）踩坑，操作DB时要使用连接池。
  */
object ForeachRDDApp {
  def main(args: Array[String]): Unit = {{
    val sc = new SparkConf()
      .setAppName("word count")
      .setMaster("local[3]")

    val ssc = new StreamingContext(sc, Seconds(5))

    val lines = ssc.socketTextStream("192.168.76.120", 1314)
    //当数据多次被使用，数据持久化
//    lines.persist(StorageLevel.MEMORY_ONLY)
    val wordsDStrem = lines.flatMap(_.split(" "))
    wordsDStrem.foreachRDD(rdd =>{
      val spark =  SparkSession
        .builder
        .config(rdd.sparkContext.getConf)
        .getOrCreate()
      import spark.implicits._
      val wordDF = rdd.toDF("word")
      wordDF.createOrReplaceGlobalTempView("words")
      val countDF = spark.sql("select word, count(*) as total from words group by word")
      countDF.show()
    })


    ssc.start()
    ssc.awaitTermination()
  }

  }

}
