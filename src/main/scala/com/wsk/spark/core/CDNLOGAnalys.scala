package com.wsk.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * CDN点击日志需求
  *
  * 需求一：完成每个域名下访问数TOP10的文件资源，按照访问次数降序排列
  * 文件资源解释：
  * url为：http://ruozedata.com/line.html  则文件资源是/line.html
  * url为：http://ruozedata.com/a/b/c/line.html?a=b&c=d， 则文件资源是/a/b/c/line.html
  * a.com  1.html   100  traffic
  * a.com  2.html   99
  * a.com  3.html   98
  * ...
  * a.com  10.html  10
  *
  * 需求二：
  * 文件资源解释：完成每个域名下资源traffic和TOP10的文件资源，按照流量降序排列
  *
  * 需求三：完成自己造的数据每个域名每个小时的流量统计
  * 造3个小时的数据
  * (2019050510,a.com, 2000)
  * (2019050511,a.com, 2100)
  * (2019050512,a.com, 2200)
  *
  */
object CDNLOGAnalys {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("CDN-log-Analysis").setMaster("local[2]")
    val sc = new SparkContext(conf)

    //    val lines = sc.textFile("data/cdnlog/input/test.log")
    val lines = sc.textFile("data/cdnlog/input/CDN_click.log")
      .map(x => x.split(","))
      .filter(_.length == 8)

    println("数据总量：" + lines.count())

    /**
      * 需求一:求每个域名下访问次数前10的资源
      *
      * 第一步 ：map=>(k，v) ；k=(域名，资源名称)，v=（1，流量）
      * 第二步：reducebykey=>(k,v)，k=(域名，资源名称)，v=（访问次数和，流量和）,计算出每个域名下每个资源的 访问次数以及流量和
      * 第三步：map=>(k，v)；k=域名，v=(资源名称，访问次数和，流量和)
      * 第四步：groupbykey=>(k，List[V]),分组，k=域名，V=(资源名称，访问次数和，流量和)
      * 第五步：map，对每个组中元素，按照一定规则排序，然后取前10位
      * 第六步：flatMapValues，将分组内容平铺
      *
      */
    val doMainGroups = lines.map(a => {
      var traffic = 0L
      try {
        traffic = a(7).toLong
      } catch {
        case e: Exception => 0L
      }
      ((a(5), getResource(a(6), a(5))), (1, traffic))
    })
      .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
      .map(x => (x._1._1, (x._1._2, x._2._1, x._2._2)))
      .groupByKey().coalesce(1).cache()

    var accessTop10 = doMainGroups.map(x => {
      val top3 = x._2.toArray.sortWith((a, b) => a._2 > b._2).take(10)
      (x._1, top3)
    }).flatMapValues(x => x)
    accessTop10.saveAsTextFile("data/cdnlog/output/job1")

    /**
      * 需求二：求每个域名下traffic和前10的资源
      * 同第一题，只有第五步的比代码有些许的差别，使用的是流量进行比较
      */
    var trafficTopN = doMainGroups.map(x => {
      val topN = x._2.toArray.sortWith((a, b) => a._3 > b._3).take(10)
      (x._1, topN)
    }).flatMapValues(x => x)
    trafficTopN.saveAsTextFile("data/cdnlog/output/job2")


    /**
      * 需求三：完成自己造的数据每个域名每个小时的流量统计
      *
      * 类似于第一题的第一二的步骤
      */
    val domainTimeRDD = lines.map(a => {
      var traffic = 0L
      try {
        traffic = a(7).toLong
      } catch {
        case e: Exception => 0L
      }
      ((a(5), getHourStr(a(3))), traffic)
    }).reduceByKey(_+_).map(x=>(x._1._1,x._1._2,x._2)).coalesce(1)
    domainTimeRDD.saveAsTextFile("data/cdnlog/output/job3")


    sc.stop()
  }

  /**
    * 获取资源路径
    *
    * @param url 请求地址，默认是包含了域名
    * @return
    */
  def getResource(url: String, doamin: String): String = {
    val urlFiles = url.split("//" + doamin+"/")
    if (urlFiles.length == 2) {
      urlFiles(1)
    }
    urlFiles(1)
  }

  /**
    * 截取时间字符串
    * @param dateStr
    * @return
    */
  def getHourStr(dateStr: String):String= {
    try{
    dateStr.substring(0,10)
    }catch {
      case e:Exception=>"1901010101"
    }
  }

}
