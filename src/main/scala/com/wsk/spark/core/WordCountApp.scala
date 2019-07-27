package com.wsk.spark.core

import org.apache.spark.internal.Logging
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 可打包后用spark-submit部署的单词计数代码
  */
object WordCountApp extends Logging{

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster("local[2]").setAppName("wc")
    val sc = new SparkContext(conf)

    val textRDD = sc.textFile("data/etlLog/input/hadoop-click-log.txt")
    val wc = textRDD.flatMap(_.split(","))
      .map((_,1))
      .reduceByKey(_+_)

    //控制台打印输出
    wc.collect().foreach(
      x =>logError(s"输出结果：${x.toString()}")
    )

    //输出到hdfs上,可以进行压缩
//    wc.saveAsTextFile(args(1));


    Thread.sleep(200000)
    sc.stop()

  }

}
