package com.wsk.spark.stream

/**
  * 使用 sparkStream 读取tcp的数据，统计单词数前十的单词
  *
  * 1）spark是以批处理为主，以微批次处理为辅助解决实时处理问题
  *   flink以stream为主，以stram来解决批处理数据
  * 2）Stream的数据过来是需要存储的，默认存储级别：MEMORY_AND_DISK_SER_2
  * 3）因为tcp需要一个线程去接收数据，故至少两个core，
  *   基本的Stream中，只有FileStream没有Receiver，其它都有，而高级的Stream中Kafka选择的是DirStream，也不使用Receiver
  * 4）Stream 一旦启动都不会主动关闭，但是可以通过WEB-UI进行优雅的关闭
  * 5）一旦start()就不要做多余的操作，一旦stop则程序不能重新start，一个程序中只能有一个StreamContext
  * 6）对DStrem做的某个操作就是对每个RDD的操作
  * 7)receiver是运行在excuetor上的作业，该作业会一直一直的运行者，每隔一定时间接收到数据就通知driver去启动作业
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
    val lines = ssc.socketTextStream("192.168.43.125", 1314)
    //对DStrem做的某个操作就是对每个RDD的每个操作
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
