package com.wsk.spark.xianxia

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * 生产核心代码
  */
object EmpEtlApp {

  def main(args: Array[String]): Unit = {

    val spark  = SparkSession.builder()
//      .appName("Proudct Analys APP")
//      .master("local[4]")
      .getOrCreate()


    //此处理时间
    val time = spark.sparkContext.getConf.get("spark.time")
    //一个task处理数据量，单位M
    val size = spark.sparkContext.getConf.get("spark.task.size").toInt
    if(time == null){
      System.exit(0)
    }
//    val time = "2019060815"

    val input = s"hdfs://192.168.175.135:9000/hadoop/offline/raw/${time}/"
    val output = "hdfs://192.168.175.135:9000/hadoop/offline/col/"

    val conf = new Configuration()
    val fs = FileSystem.get(new URI("hdfs://192.168.175.135:9000"), conf)
    val logDF = spark.read.format("text").load(input).coalesce(FileUtil.getCoalesce(fs,input+"*",size))
    val eltDF = spark.createDataFrame(logDF.rdd.map(x =>{EmpParser.parseLog(x.getString(0))}),EmpParser.struct)

    eltDF.write.format("parquet")
      .option("compression","none")
      .partitionBy("day","hour")
      .mode(SaveMode.Overwrite)
      .save(output)
//    eltDF.show()

    Thread.sleep(30000)
    spark.stop()
  }
}
