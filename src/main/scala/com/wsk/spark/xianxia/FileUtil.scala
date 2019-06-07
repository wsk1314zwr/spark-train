package com.wsk.spark.xianxia

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.internal.Logging

object FileUtil extends Logging{

  def getCoalesce(fs: FileSystem, path: String, site: Int): Int = {

    var length = 0L
    fs.globStatus(new Path(path)).map(x => {
      length += x.getLen
    })
    val result = (length / 1024 / 1024 / site).toInt + 1
    logError(s"path:${path},length:${length},site:${site},partition:${result }")

    result
  }

  def main(args: Array[String]): Unit = {

    val conf = new Configuration()
    // 写代码：死去活来法
    val fs = FileSystem.get(new URI("hdfs://192.168.175.135:9000"), conf)
    val pattitionNum = getCoalesce(fs, "/hadoop/offline/raw/2019060815/*", 50)

    println(pattitionNum)
  }

}
