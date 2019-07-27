package com.wsk.spark.stream

import org.apache.spark.SparkConf
import org.apache.spark.streaming._

/**
  * updateStateByKey Transformations,实现夸批次的wordcount
  *
  * 1)MapWithState可以实现和UpdateStateBykey一样对不同批次的数据的分析，但是它是实验性方法，慎用，可能下一版本就没了
  * 2）MapWithState，只有当前批次出现了该key才会显示该key的所有批次的分析数据
  */
object MapWithStateTest {
  def main(args: Array[String]) {

    //第一个参数是key，第二参数是当前value,第三个参数之前的value
     val mappingFunction = (key: String, value: Option[Int], state: State[Int])=> {
       val sum = value.getOrElse(0)+state.getOption().getOrElse(0)
       state.update(sum)
       (key,sum)
     }

    val sparkConf = new SparkConf()
      .setAppName("StatefulNetworkWordCount")
      .setMaster("local[3]")
    // Create the context with a 5 second batch size
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    ssc.checkpoint(".")

    // Initial RDD input to updateStateByKey
    val initialRDD = ssc.sparkContext.parallelize(List(("hello", 1), ("world", 1)))
    // Create a ReceiverInputDStream on target ip:port and count the
    // words in input stream of \n delimited test (eg. generated by 'nc')
    val lines = ssc.socketTextStream("192.168.76.120", 1314)
    val words = lines.flatMap(_.split(" ")).map(x => (x, 1)).reduceByKey(_+_)
    val stateDstream = words.mapWithState(StateSpec.function(mappingFunction))

    stateDstream.print()


    ssc.start()
    ssc.awaitTermination()
  }
}