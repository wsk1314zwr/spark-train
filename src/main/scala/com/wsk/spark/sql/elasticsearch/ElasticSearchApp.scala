package com.wsk.spark.sql.elasticsearch

import java.io.File
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession

object ElasticSearchApp {
    def main(args: Array[String]): Unit = {
        val warehouseLocation = new File("spark-warehouse").getAbsolutePath

        val spark = SparkSession
                .builder()
                .master("local[1]")
                .appName("Spark Hive Example")
                .config("spark.sql.warehouse.dir", warehouseLocation) //使用本地的hive
                .enableHiveSupport()
                .getOrCreate()

        val sqlScript = FileUtils.readFileToString(new File("src/main/resources/sql/spark-es.sql"), "utf8")
        sqlScript.split(";").foreach(sql => {
            System.out.println(sql)
            spark.sql(sql)
        })
        spark.stop()
    }
}
