package com.wsk.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 获取男女最低和最高的身高 假设列0 的值0表示男 1表示女，列1 表示对应的身高
  */
object PeopleHight {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val sc = new SparkContext(conf)

    val rdd = sc.textFile(args(0)).
      map(x => (x.split(",")(0).toInt,x.split(",")(1).toFloat)).cache()
    //第一步将RDD拆为男和女两个Rdd

    //第二步分别求男女人数和各自最低最高

    sc.stop()
  }

}
