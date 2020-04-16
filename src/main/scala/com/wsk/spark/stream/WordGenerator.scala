package com.wsk.spark.stream

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.util.Random


/**
  */
object WordGenerator {

  val bootstrapServers = "192.168.175.135:9092"

  def main(args: Array[String]): Unit = {


    val properties = new Properties()
    properties.setProperty("bootstrap.servers",bootstrapServers)
    properties.setProperty("key.serializer","org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty("value.serializer","org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty("acks","-1")

    val kafkaProducer = new KafkaProducer[String,String](properties)

    for (i<-1 to 10000) {
//      Thread.sleep(100)
      val word = String.valueOf((new Random().nextInt(6) + 'a').toChar)
      val part = i % 3
//      println("...." + word)
      val producerRecord = new ProducerRecord[String,String]("wsk_test_3", part, "",word)
      kafkaProducer.send(producerRecord)
    }
    kafkaProducer.flush()
    kafkaProducer.close()

  }
}
