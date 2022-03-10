package com.wsk.spark.cor

import org.apache.spark.sql.SparkSession


object CustomOptimizerRules {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession
                .builder()
                .appName("Spark SQL basic example")
                .master("local[2]")
                .getOrCreate()

        //使用spark默认的逻辑优化规则
        val df = spark.range(10).toDF("counts")
        val df2 = df.selectExpr("1 * counts")
        print(df2.queryExecution.optimizedPlan.numberedTreeString)    //输出其逻辑执行计划，
        //逻辑执行计划中还是有1 * id#0L 优化做的不好，需继续优化，遇到1 *  id#0L就应该是其自己
        //00 Project [(1 * id#0L) AS (1 * counts)#4L]
        //01 +- Range (0, 10, step=1, splits=Some(2))

        //使用自定义的逻辑优化规则
        spark.experimental.extraOptimizations ++= MultiplyOptimizationRule :: Nil
        val df3 = spark.range(20).toDF("counts")
        val df4 = df3.selectExpr("1 * counts")
        print(df4.queryExecution.optimizedPlan.numberedTreeString)    //输出其逻辑执行计划，
        //逻辑执行计划中1 * id#0L已经被优化为id#0L了
        //00 Project [id#6L AS (1 * counts)#10L]
        //01 +- Range (0, 20, step=1, splits=Some(2))
        spark.stop()
    }

}
