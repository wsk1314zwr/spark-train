package com.wsk.spark.stream

import javax.sql.ConnectionPoolDataSource
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * window -Transformations 实现单词计数，多个批次的滑动联合展示
  */
object WindowTFTest {
  def main(args: Array[String]): Unit = {
    val sc = new SparkConf()
      .setAppName("word count")
      .setMaster("local[3]")

    val ssc = new StreamingContext(sc, Seconds(5))

    val lines = ssc.socketTextStream("192.168.76.120", 1314)
    val wordContDS = lines.flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)
        wordContDS.print()
    //window
//    val windowDS = wordContDS.window(Seconds(20), Seconds(10))
//    windowDS.print()

    //save
//    wordContDS.saveAsTextFiles("C:\\Users\\admin\\Desktop\\spark学习\\outPutData\\")

    ssc.start()
    ssc.awaitTermination()
  }

}
