package com.wsk.spark.fixfile

import org.apache.hadoop.io.{BytesWritable, LongWritable}
import org.apache.hadoop.mapreduce.lib.input.FixedLengthInputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 定长文件解析，接触MR的FixedLengthInputFormat类
 */
object FixedLengthFileApp {
    val recordLength = "2"
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[2]").setAppName("FixedLengthFileTest")
        val sc = new SparkContext(conf)
        val configuration = sc.hadoopConfiguration
        configuration.set(FixedLengthInputFormat.FIXED_RECORD_LENGTH, recordLength)
        val rdd = sc.newAPIHadoopFile("data/fixFile/fixFile.txt",
            classOf[FixedLengthInputFormat],
            classOf[LongWritable],
            classOf[BytesWritable],
            configuration
        ).map(x => new String(x._2.getBytes))
        rdd.foreach(println)

        val repartitionRdd = rdd.repartition(2)

        //方案一，隐士转换
//        implicit class WskRdd(rdd:RDD[String]){
//            def saveAsTextFileCnt(path:String): Long = {
//                val accumulator = rdd.sparkContext.longAccumulator
//                rdd.map(x =>{
//                    accumulator.add(1L)
//                    x
//                }).saveAsTextFile(path)
//                val cnt = accumulator.value
//                cnt
//            }
//        }
        if (repartitionRdd.isEmpty()) {
            //TODO..
        } else {
            //            repartitionRdd.saveAsTextFile("data/fixFile/outFile")
//            val cnt = repartitionRdd.saveAsTextFileCnt("data/fixFile/outFile") //方案一，隐士转换
            val cnt = saveAsTextFileCnt("data/fixFile/outFile", rdd) //方案二，自定义函数
            println(cnt) //可以将结果输出到外部，用于对数
        }

        sc.stop()
    }


    //方案二，自定义函数
    def saveAsTextFileCnt(path: String, rdd: RDD[String]): Long = {
        val accumulator = rdd.sparkContext.longAccumulator
        rdd.map(x => {
            accumulator.add(1L)
            x
        }).saveAsTextFile(path)
        val cnt = accumulator.value
        cnt
    }

}
