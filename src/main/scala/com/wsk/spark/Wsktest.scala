package com.wsk.spark

import org.apache.spark.{SparkConf, SparkContext}

object  Wsktest{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("wsktest-20181127").setMaster("local[2]")
    val sc = new SparkContext(conf)

    /**
      * 创建RDD
      */
//      方法一：a
    val collecton = Array(1,2,3,4,5,6)
    val paraRDD = sc.parallelize(collecton)
    paraRDD.collect()
    paraRDD.reduce((a, b) => a + b)
    var str = "hello world"
    //创建带指定分区的RDD，默认是两个分区
//    val paraRDD = sc.parallelize(collecton)

//    方法二：从已有的文件系统创建RDD
    //读取linux本地文件
//    val stringRDD = sc.textFile("file:///root/data/testDaa.txt")
    //读取windows本地文件
    val stringRDD = sc.textFile("C:\\Users\\admin\\Desktop\\spark学习\\spark零基础到实战\\testData.txt")
    //读取hdfs文件
//    val stringRDD = sc.textFile("hdfs://ip:port/testData.txt")
    System.out.println(stringRDD.collect())
    System.out.println(stringRDD.map(s => s.length).reduce((a, b) => a + b))

    val a = sc.parallelize(Array("dog","tiger","lion","cat","panda")).flatMap(x=>Array(x,x+ 1)).collect()

    sc.stop()
  }
}
