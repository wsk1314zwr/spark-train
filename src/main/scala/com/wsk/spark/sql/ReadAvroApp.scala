package com.wsk.spark.sql

import org.apache.avro.generic.GenericRecord
import org.apache.avro.mapred.{AvroInputFormat, AvroWrapper}
import org.apache.hadoop.io.NullWritable
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

/**
  * 读取AVRO文件
  */
object ReadAvroApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("ReadAvroApp")
      .getOrCreate()
    spark.sparkContext.hadoopConfiguration.set("avro.mapred.ignore.inputs.without.extension", "false")
    val df = spark.read.format("com.databricks.spark.avro").load("exampleData/users.avro")
    df.printSchema()
//    val conf = new SparkConf().setAppName("log_deal").setMaster("local[2]")
//    val sc = new SparkContext(conf)
//    val avroRDD = sc.hadoopFile[AvroWrapper[GenericRecord], NullWritable, AvroInputFormat[GenericRecord]]("exampleData/FlumeData.avro")
//    avroRDD.map(l => {
//      val line = l._1.toString
//      line
//    }
//    ).foreach(println)
//

    spark.stop()

  }

}
