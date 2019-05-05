package com.wsk.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 获取访问网页的人得平均年龄 ,假设列1是年龄 一行代表一个人
  */
object AveAges {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    val sc = new SparkContext(conf)

    val textRDD = sc.textFile(args(0))

    var restTupl = textRDD.map(x => (x.split(",")(0).toInt,1))
        .reduce((x,y)=>(x._1+y._1,x._2+y._2))

    println("打印输出结果 ：" + (restTupl._1/restTupl._2))


    sc.stop()
  }

}
