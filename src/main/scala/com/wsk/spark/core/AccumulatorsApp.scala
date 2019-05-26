package com.wsk.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * share variables:AccumulatorsApp
  *  累加器测试
  */
object AccumulatorsApp {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("AccumulatorsApp")
    val sc = new SparkContext(conf)

    val wskAccum = sc.longAccumulator("wsktssets")
    val nums = sc.parallelize(List(1,2,3,4),2)
    println("driver端:计数器值："+wskAccum.value)

    nums.map(x=>{
      println("计数前值："+wskAccum.value)
      wskAccum.add(x)
      println("计数后值："+wskAccum.value)
      x
    }).collect()
    println("driver端:最终计数器值："+wskAccum.value)

    Thread.sleep(2000000)

    sc.stop()
  }
}
