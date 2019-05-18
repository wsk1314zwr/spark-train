package com.wsk.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 以saprk core实现窗口函数first_value的功能
  * 当前的firstvalue是没有进行组内排序的，若要组内排序也是非常简单的，将Iterable转成list然后再按照一定规则去排序
  */
object FirstValueApp {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("FirstValueApp").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val data = sc.parallelize(List(
      ("A", "A1"),
      ("A", "A2"),
      ("A", "A3"),
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
    但是这时候core中并没有直接实现可以拿到first value 。
    那我们要做的是拿到
    (A,List((A1,A1), (A2,A1), (A3,A1)))
    (B,List((B1,B1), (B2,B1), (B3,B1)))
    (C,List((C1,C1)))
    即，迭代出第一个元素即可
    (A,A1,A1)
    (A,A2,A1)
    (A,A3,A1)
     */
    data.groupByKey().sortByKey()
      .map(x => (x._1, firstValue(x._2))).flatMap(x => {
      for(value <- x._2) yield{
        (x._1,value._1,value._2)
      }
    })
      .collect().foreach(println)

    //自定义一个方法
    //进来一个迭代器，输出一个firstvalue
    def firstValue(values: Iterable[String]) = {

      for (v <- values) yield {
        (v, values.head)
      }


    }

    sc.stop()
  }
}
