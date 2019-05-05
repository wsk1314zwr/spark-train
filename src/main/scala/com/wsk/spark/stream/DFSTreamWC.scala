package com.wsk.spark.stream

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DFSTreamWC {
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
