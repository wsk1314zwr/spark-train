package com.wsk.spark.stream

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{KafkaUtils, _}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, TaskContext}


/**
  */
object DirectKafkaApp {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[3]")
      .setAppName("DirectKafkaApp")
      .set("spark.streaming.kafka.maxRatePerPartition","100")
      .set("spark.streaming.backpressure.enabled","true")
      .set("spark.streaming.stopGracefullyOnShutdown","true")

    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val kafkaParams = Map[String, Object](
//      "bootstrap.servers" -> "hadoop000:9092,hadoop000:9093,hadoop000:9094",
      "bootstrap.servers" -> "10.199.151.15:9092,10.199.151.16:9092,10.199.151.17:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "test_spark_groupid",
      "auto.offset.reset" -> "earliest",  //earliest
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    // 如果多个topic，就使用逗号分隔
    val topics = Array("wsk_test_2019")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    stream.foreachRDD { rdd =>

      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd.foreachPartition { iter =>
        val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
        println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }

}
