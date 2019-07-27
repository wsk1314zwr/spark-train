package com.wsk.spark.stream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 以transform实现白名单操作：
  *
  * 1）Transform是Transformation类型操作，用它可以将DStream转换成RDD，这样我们可以通过RDD的编程
  * 去实现业务逻辑，如白名单过滤
  */
object TransformApp {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("Transform App")

    //每隔一秒的数据为一个batch
    val ssc = new StreamingContext(conf, Seconds(5))


    val whiteRDD = ssc.sparkContext.parallelize(List("17")).map((_, true))
    //读取的机器以及端口,读取的数据的格式是：老二,3,1 （姓名，年龄，性别）
    val lines = ssc.socketTextStream("192.168.43.125", 1314)

    val result = lines.map(x => (x.split(",")(0), x))
      .transform(rdd => {
        //(老二,((老二,3,1),Option[]))
        rdd.leftOuterJoin(whiteRDD)
          .filter(_._2._2.getOrElse(false) != true)
          .map(_._2._1)
      })
    result.print()

    ssc.start() // Start the computation
    ssc.awaitTermination() // Wait for the computation to terminate
  }


}
