package com.wsk.spark.core

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer


/**
  * share variables:BroadcastApp
  *  广播大变量实现以map join取代reduce join
  */
object BroadcastApp {

  def main(args: Array[String]): Unit = {
//    commomJoin()

    broadcastJoin()
  }
  def commomJoin(): Unit = {

    val conf = new SparkConf().setMaster("local[2]").setAppName("BroadcastApp")
    val sc = new SparkContext(conf)

    val wideInfo = sc.parallelize(List(("01","阿呆"),("02","sk"),("03","shjqi")))
    val baseInfo = sc.parallelize(List(("01",("北京","22")),("05",("上海","22"))))

    wideInfo.join(baseInfo).map(x =>(x._1,x._2._1,x._2._2._1,x._2._2._2))
      .collect().foreach(println)

    Thread.sleep(2000000)
    sc.stop()
  }

  def broadcastJoin(): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("BroadcastApp")
    val sc = new SparkContext(conf)

    val wideInfo = sc.parallelize(List(("01","阿呆"),("02","sk"),("03","shjqi")))
    val baseInfo = sc.parallelize(List(("01",("北京","22")),("05",("上海","22"))))
      .collectAsMap()
    val broadcastBaseInfo = sc.broadcast(baseInfo)

    wideInfo.mapPartitions(x =>{
      val value = broadcastBaseInfo.value
      for((k,v)<-x if(value.contains(k)))
        yield (k,v,value.get(k).getOrElse().asInstanceOf[(String,String)]._1,value.get(k).getOrElse().asInstanceOf[(String,String)]._2)
    }).foreach(println)

    Thread.sleep(2000000)
    sc.stop()
  }
}
