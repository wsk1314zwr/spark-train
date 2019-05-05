package com.wsk.spark.stream

/**
  * 使用 sparkStream 读取tcp的数据，统计单词数前十的单词
  */

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.PairDStreamFunctions
import org.apache.spark.streaming.{Seconds, StreamingContext}
object TcpStream {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("word count")

    //每隔一秒的数据为一个batch
    val ssc = new StreamingContext(conf,Seconds(5))
    //读取的机器以及端口
    val lines = ssc.socketTextStream("192.168.76.120", 1314)
    //正常的RDD转换操作，是lazy的
    val words = lines.flatMap(_.split(" "))
    val pair = words.map(word =>(word,1))

    //更新key的状态，

    val wordCounts = pair.reduceByKey((x,y)=>x+y)
    // Print the first ten elements of each RDD generated in this DStream to the console
    wordCounts.print()


    ssc.start()  // Start the computation
    ssc.awaitTermination() // Wait for the computation to terminate
  }



}
