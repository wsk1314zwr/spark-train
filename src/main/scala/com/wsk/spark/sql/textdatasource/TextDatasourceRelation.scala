package com.wsk.spark.sql.textdatasource

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

/**
  * 读取外部数据以及保存
  */
class TextDatasourceRelation(override val sqlContext: SQLContext,
                             path:String,
                             userSchema:StructType)
  extends BaseRelation with TableScan
//  with PrunedScan with PrunedFilteredScan
    with Logging{

  override def schema: StructType = {
    if(userSchema != null){
      userSchema
    } else {
      StructType(
        StructField("id",LongType,false) ::
          StructField("name",StringType,false) ::
          StructField("gender",StringType,false) ::
          StructField("salary",LongType,false) ::
          StructField("comm",LongType,false) :: Nil
      )
    }
  }

  // select * from xxx
  override def buildScan(): RDD[Row] = {
    logError("this is ruozedata custom buildScan...")

    var rdd = sqlContext.sparkContext.wholeTextFiles(path).map(x => {
      println(x)
      x._2
    })
    val schemaField = schema.fields

    rdd.foreach(println)
    // rdd + schemaField

    val l = rdd.count()
    // rdd + schemaField
    val rows = rdd.map(fileContent => {
      val lines = fileContent.split("\n")
      val data = lines.map(_.split(",").map(x=>x.trim)).toSeq

      val result = data.map(x => x.zipWithIndex.map{
        case  (value, index) => {

          val columnName = schemaField(index).name

          Utils.castTo(if(columnName.equalsIgnoreCase("gender")) {
            if(value == "0") {
              "男"
            } else if(value == "1"){
              "女"
            } else {
              "未知"
            }
          } else {
            value
          }, schemaField(index).dataType)
        }
        })
      result.map(x => Row.fromSeq(x))
    })

    rows.flatMap(x=>x)
}

//  override def buildScan(requiredColumns: Array[String]): RDD[Row] = ???
//
//  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = ???
}
