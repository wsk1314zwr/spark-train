package com.wsk.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 单词统计并排序
  */
object SortWorldCount {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    val sc = new SparkContext(conf)

    val textRDD = sc.textFile(args(0))
    val wc = textRDD.flatMap(_.split("\\s+"))
      .map((_,1))
      .reduceByKey(_+_)

    //控制台打印输出
    //    wc.collect().foreach(println)
    val sortWc = wc.sortBy(_._2.toInt,false)
    //输出到hdfs上,可以进行压缩
    sortWc.collect().foreach(x =>println("打印输出 ："+x))
    sortWc.saveAsTextFile(args(1))

    sc.stop()

  }



}
