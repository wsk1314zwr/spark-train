package com.wsk.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * ShuffleManager，注意该代码在2.0版本使用无效，最好是1.6
  *
  * 1）HashShufflemanager:1.2前默认。2.0版本废弃
  * 2)SortShuffleManager: 1.2后默
  */

object ShuffleManager {


  def main(args: Array[String]): Unit = {

    /** HashShufflemanager生成的临时文件数测试
      *
      */
    hashShuffleManagerFilesNumTest()
  }

  def hashShuffleManagerFilesNumTest(): Unit = {
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("HashShuff App")
      .set("spark.local.dir", "/tmp")
      .set("spark.shuffle.manager","HASH")
    val sc = new SparkContext(conf)
    val rdd = sc.textFile("data/cdnlog/input/CDN_click.log", 10)
    rdd.flatMap(_.split(",")).coalesce(4).repartition(6).collect()
    Thread.sleep(2000000)
    sc.stop()
  }

}
