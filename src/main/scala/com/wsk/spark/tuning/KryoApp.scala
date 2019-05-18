package com.wsk.spark.tuning

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 使用kryo序列化数据对象,
  * 1、生产山spark.serializer这个配置直接写到spark-default配置文件中即可
  */
object KryoApp {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Kyro App").setMaster("local[2]")
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        conf.registerKryoClasses(Array(classOf[Logger]))


    val sc = new SparkContext(conf)
    val logRDD = sc.textFile("data/cdnlog/input/CDN_click.log")
      .map(x => {
        val logFields = x.split(",")
        Logger(logFields(0), logFields(1), logFields(2), logFields(3),
          logFields(4), logFields(5), logFields(6), logFields(7))
      })


    //    val pesisitRDD = logRDD.persist(StorageLevel.MEMORY_ONLY)
    val pesisitRDD = logRDD.persist(StorageLevel.MEMORY_ONLY_SER)


    println("总条数" + pesisitRDD.count())


    Thread.sleep(2000000)
    sc.stop()

  }

  case class Logger(filed1: String, filed2: String, filed3: String,
                    filed4: String, filed5: String, filed6: String,
                    filed7: String, filed8: String)

}
