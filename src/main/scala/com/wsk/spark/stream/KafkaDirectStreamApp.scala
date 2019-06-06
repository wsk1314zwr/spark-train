package com.wsk.spark.stream

import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.{Seconds, StreamingContext}

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

/**
  * KakkaStream 实现WC
  * 1）在Spark2.3.0版本中Kafka0.8.0版本被标记为过时
  * 2）生产中对接Kafka版本最低选择kafka0.10.0。该版本的Stream 是Direct Stream，
  * 下面说的内容都是基于Kafka direct Stream。 receiver太古老了,未来肯定是放弃的
  * 3）(重要)Kafka direct Stram 产生的数据分区数和Kafka的分区数据数是1:1
  * 4)小胖包： 本app用到了org.apache.spark.streaming.kafka010，但是该jars spark是不自带的，
  * 故需要使用提交作业时使用 --jars传入，但是当很多jar时并不方便，最好的方式是在打打胖包，在不需要的jar的maven中将其标志位 <scope>provided</scope>
  * ，这样我们只将我们需要的jar打到Jar中，且不需要--jars配置一堆jar包，非常方便，注意：生产坚决不要使用大胖包，可能会有各种冲突问题产生，导致程序停止。
  *    第一步：不需要的打包的jar标注为provided
  *    第二步：添加assembly打包插件
  *    第三步：配置assembly打包脚本，并打包
  * 5)LocationStrategies：定位策略
  *   PreferConsistent：表示能将缓存的数据分发到不同的Exector上，生产用这种；
  *   PreferBrokers：表示Kafka的broker和Spark Exectorx有相同的hostname上，优先消费本地的lead数据，生产上可能性为0
  *   PreferFixed：当前分区间数据由显著的额数据倾斜( significant skew),使用该策略，可定位消费分区。
  *
  * 6）auto.offset.reset:若kafka没有该消费者组对该topic的消费offset位移信息
  *   latest：表示从最新的开始,即队列尾端消费
  *   earliest：表示从0开始，即队列头端消费
  *   none:抛出异常
  *
  */
object KafkaDirectStreamApp extends Logging{


  def main(args: Array[String]): Unit = {

    if (args.length != 3) {
      logError("Usage:KafkaDirectStreamApp <brokers> <topics> <groupid>")
      System.exit(0)
    }
    val Array(brokers,topic,groupid) = args

    val sc = new SparkConf()
      //本地测试需要放开如下配置
     // .setAppName("Kafka Stream App")
     // .setMaster("local[2]")

    val ssc = new StreamingContext(sc, Seconds(5))


    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupid,
      "auto.offset.reset" -> "latest", //earliest
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = topic.split(",")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    //单词统计
    stream.flatMap(_.value().split(","))
      .map((_, 1))
      .reduceByKey(_ + _)
      .print()

    //获取消费Kafka本批次的信息
    stream.foreachRDD { rdd =>
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd.foreachPartition { iter =>
        val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
        println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")

        //手动提交Offset
        stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)

      }
    }




    ssc.start()
    ssc.awaitTermination()

  }


}
