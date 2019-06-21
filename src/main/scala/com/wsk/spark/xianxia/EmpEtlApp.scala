package com.wsk.spark.xianxia

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * 生产核心代码
  */
object EmpEtlApp extends Logging {

  def main(args: Array[String]): Unit = {




    val spark = SparkSession.builder()
      .appName("Proudct Analys APP")
      .master("local[4]")
      .getOrCreate()


    val beginingTime = System.currentTimeMillis()
    val appName = spark.sparkContext.getConf.get("spark.app.name")

    //此处理时间
    val time = "2019060810"
    //    val time = spark.sparkContext.getConf.get("spark.time")
    //    if (time == null) {
    //      System.exit(0)
    //    }

    //一个task处理数据量，单位M
    val size = spark.sparkContext.getConf.get("spark.task.size", "128").toInt

    val input = s"hdfs://192.168.175.135:9000/hadoop/offline/raw/${time}/"
    val output = s"hdfs://192.168.175.135:9000/hadoop/offline/col/"

    //机器上跑，可直接使用hadoopConfiguration
    //      val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    val conf = new Configuration()
    val fs = FileSystem.get(new URI("hdfs://192.168.175.135:9000"), conf)

    try {
      val totalNum = spark.sparkContext.longAccumulator("totalNum")
      val errorNum = spark.sparkContext.longAccumulator("errorNum")

      val logDF = spark.read.format("text").load(input).coalesce(FileUtil.getCoalesce(fs, input + "*", size))
      val eltDF = spark.createDataFrame(logDF.rdd.map(x => {
        EmpParser.parseLog(x.getString(0), totalNum, errorNum)
      }).filter(_.length != 1), EmpParser.struct)

      //将数据写到临时的目录 baseURL/2018091010/day=20180910/hour=10/xxx-1.log
      //                   baseURL/2018091010/day=20180910/hour=11/xxx-1.log
      eltDF.write.format("parquet")
        .option("compression", "none")
        .partitionBy("day", "hour")
        .mode(SaveMode.Overwrite)
        .save(output + time)
      //    eltDF.show()


      //TODO.. mv 前，删除所有该批次的文件 ,保证任务无论是重跑、修复多少次，ETL的结果正确性


      //TODO...mv 中，将文件rename到真实的目录，带上批次前缀
      //  baseURL/day=20180910/hour=10/2018091010-1.log
      //  baseURL/day=20180910/hour=11/2018091010-1.log

      val toltalCount = totalNum.count
      val errorCount = errorNum.count
      val totalTime = System.currentTimeMillis() - beginingTime
      logError(s"$appName 运行成成功，读取数据总量：$toltalCount，失败数据量$errorCount,耗时：$totalTime")
      //TODO .. 将统计的日志信息通过Http发送到ES，做大屏监控
    } catch {
      case e: Exception => logError(s"$appName 运行失败,异常消息：${e.printStackTrace()}")
    } finally {
      //TODO...mv 后，删除 文件夹  SparkHadoopUtil
      //SparkHadoopUtil.get.globPath(new Path(output+time)).map(fs.delete(_,true))

      Thread.sleep(30000)
      if (spark != null) {
        spark.stop()
      }
    }


  }
}
