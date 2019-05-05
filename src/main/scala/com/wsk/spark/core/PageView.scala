package com.wsk.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 进行网页用户访问量topN的编程，假设列2是被访问的网站地址，一行代表一条访问记录
  *
  */
object PageView {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val pageViewRDD = sc.textFile(args(0))
    val topNRDD = pageViewRDD.map(x => (x.split(",")(1), 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, false)

    //控制台打印输出
    topNRDD.foreach(x =>println("打印输出结果："+ x))

    sc.stop()

  }

}
