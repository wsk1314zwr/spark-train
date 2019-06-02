package com.wsk.spark.sql.datasource.customdatasource

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

import scala.collection.mutable.ListBuffer

class TextDataSourceRelation(sqlcontext: SQLContext
                             , path: String
                             , userSchema: StructType
                            ) extends BaseRelation with TableScan with Logging {
  override def sqlContext: SQLContext = {
    sqlcontext
  }

  override def schema: StructType = {
    if(userSchema != null){
      userSchema
    }else{
      StructType(StructField("id",LongType,false)::
        StructField("name",StringType,false)::
        StructField("gender",StringType,false)::
        StructField("salary",LongType,false)::
        StructField("com",LongType,false)::Nil)

    }
  }

  override def buildScan(): RDD[Row] = {
    logInfo("this is wsk custom test")
    var textRDD = sqlcontext.sparkContext.wholeTextFiles(path).map(_._2)
    var fileds = schema.fields
    val value: RDD[Row] = textRDD.flatMap(textContext => textContext.split("\n")).map(x => {
      var fieldSeq = x.split(",").map(x => x.trim).toSeq
      //方法一，此方法写死了列数，不可取
//      Row(fileds(0).toLong,fileds(1),fileds(2),fileds(3).toLong,fileds(4).toLong)
      //方法二，通过循环schema filed集合的方式
      val listBuf = new ListBuffer[Any]
      for(x <- 0 until fileds.size){
        fileds(x).dataType match {
          case LongType =>listBuf += fieldSeq(x).toLong
          case StringType => listBuf +=fieldSeq(x).toString
          case _ => throw new IllegalArgumentException("位置的字段类型")
        }
      }
//      listBuf.foreach(println)
      Row(listBuf:_*)
    })
    value

  }
}
