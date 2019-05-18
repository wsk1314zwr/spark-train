package com.wsk.spark.core

import org.apache.spark.{SparkConf, SparkContext}

object LastValueApp {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("FirstValueApp").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val data = sc.parallelize(List(
      ("A", "A3"),
      ("A", "A2"),
      ("A", "A1"),
      ("B", "B1"),
      ("B", "B2"),
      ("B", "B3"),
      ("C", "C1")
    ))
    //data.collect().foreach(println)
    /*首先进行分组，在进行排序，结果如下：
    (A,CompactBuffer(A1, A2, A3))
    (B,CompactBuffer(B1, B2, B3))
    (C,CompactBuffer(C1))
    但是这时候core中并没有直接实现可以拿到lastValue 。
    那我们要做的是拿到
    (A,List((A1,A3), (A2,A3), (A3,A3)))
    (B,List((B1,B3), (B2,B3), (B3,B3)))
    (C,List((C1,C1)))
    即，迭代最后一个元素即可
     */
    data.groupByKey().sortByKey()
      .map(x => (x._1, lastValue(x._2)))
      .flatMap(x => {
      for (v <- x._2) yield {
        (x._1, v._1, v._2)
      }
    })
      .collect().foreach(println)

    //自定义一个方法
    //进来一个迭代器，输出一个lastValue
    def lastValue(values: Iterable[String]) = {
      for (value <- values) yield {
        (value, values.last)
      }

    }

    sc.stop()
  }
}
