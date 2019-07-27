package com.wsk.spark.stream

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.util.Random


/**
  */
object WordGenerator {


  def main(args: Array[String]): Unit = {


    val properties = new Properties()
    properties.setProperty("bootstrap.servers","10.199.151.15:9092,10.199.151.16:9092,10.199.151.17:9092")
    properties.setProperty("key.serializer","org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty("value.serializer","org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty("acks","-1")

    val kafkaProducer = new KafkaProducer[String,String](properties)

    for (i<-1 to 10000) {
      Thread.sleep(100)
      val word = String.valueOf((new Random().nextInt(6) + 'a').toChar)
      val part = i % 3
//      println("...." + word)
      val producerRecord = new ProducerRecord[String,String]("wsk_test_2019", part, "",word)
      kafkaProducer.send(producerRecord)
    }



  }

}
