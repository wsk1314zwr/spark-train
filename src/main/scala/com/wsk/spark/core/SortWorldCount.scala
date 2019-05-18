package com.wsk.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 单词统计并排序
  */
object SortWorldCount {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    val sc = new SparkContext(conf)



    val nums = sc.parallelize(List(1,3,5,7,8))
    println(nums.partitions.length)
    nums.foreach(println)



    sc.parallelize(List(1,1,2,23,1,4,3)).map((_,1)).reduceByKey(_+_).repartition(1).groupByKey()
    val textRDD = sc.textFile(args(0)).repartition(2)
    val wc = textRDD.flatMap(_.split("\\s+"))
      .map((_,1))
      .reduceByKey(_+_)
    wc.cache()
    wc.unpersist()

    //控制台打印输出
    //    wc.collect().foreach(println)
    val sortWc = wc.sortBy(_._2.toInt,false)
    //输出到hdfs上,可以进行压缩
    sortWc.collect().foreach(x =>println("打印输出 ："+x))
    sortWc.saveAsTextFile(args(1))

    sc.stop()

  }



}
