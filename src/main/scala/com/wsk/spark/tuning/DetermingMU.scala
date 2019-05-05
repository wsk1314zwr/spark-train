package com.wsk.spark.tuning

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 确定内存的使用：
  *   0、在执行日志中观看:
  *   INFO MemoryStore: Block rdd_0_1 stored as values in memory (estimated size 2.9 MB, free 360.7 MB)
  *   INFO BlockManagerInfo: Added rdd_0_1 in memory on 192.168.153.130:50815 (size: 2.9 MB, free: 360.7 MB)
  *   1、cache，persi 后web ui上观看 storage页面 。 成功，在页面上看到rdd在menmory和disk中的大小
  *   2、使用SizeEstimator’s 预估内存使用 。
  */

object DetermingMU {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("appName").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val listBuffer = collection.mutable.ListBuffer[String]()

    println("优化测试开始装填数据")
    for(x <- 1 until 16){
      listBuffer +=x.toString
    }
    val list = listBuffer.toList
    println("优化测试填数据结束，集合大小1："+list.length)
    //持久化数据到内存，使得多次使用该RDD不会重新计算
//    val value = sc.parallelize(list).cache()
    //序列化方式存储数据减少内存时间,增加访问对象成本,减少了GC开销
//val value = sc.parallelize(list).persist(StorageLevel.MEMORY_ONLY_SER)
    val value = sc.parallelize(list).persist(StorageLevel.MEMORY_AND_DISK)
    value.collect().foreach(x =>println("打印："+x))
    println("优化测试填数据结束，集合大小2："+value.count())


    Thread.sleep(1000*60*2)
    sc.stop()
  }
}
