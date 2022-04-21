package com.wsk.spark.sql.elasticsearch

import com.wsk.spark.kerberos.HdfsKerberos
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SparkSession

object ElasticSearchApp {
    def main(args: Array[String]): Unit = {
        //若resources下有hfds相关的配置文件，若配置文件中配置了kerberos认证登录，则需要开启kerberos认证
//        HdfsKerberos.kerberos(new Configuration())

        // 通过sql 完成对es的读写操作
        val spark = SparkSession.builder()
                .appName("wsk-es-test")
                .master("local[2]")
                .getOrCreate()
        val create_left = spark.sql(
            """
              |CREATE TEMPORARY view mirrior_left
              |USING org.elasticsearch.spark.sql
              |OPTIONS (resource 'mysearch-mirror-md_cbb_info_dev-v4/_doc',
              |          nodes '10.199.151.14', port '9200', net.http.auth.user 'mysearch',
              |          net.http.auth.pass 'mysearch_123')""".stripMargin)
        // 解析 binary数据会报错，将ScalaValueReader.scala 92行移除json解析应该能解决问题，但是由于是grandle项目，暂时放弃修改es-hadoop的源码,海洋已经更改那个源码了
//        val frame = spark.sql("select  `customer_id`,`@del`,`@mt`,string(mirrior_left.`@data`) from mirrior_left")
//        val rows = frame.collect()
//        rows.foreach(println(_))

//        val mirror1_frame = spark.sql("select `customer_type`,`@del`,`@mt`,`@id`,`id`,`customer_id` from mirrior_left")
//        mirror1_frame.printSchema()
//        val rows = mirror1_frame.collect()
//        rows.foreach(println(_))

        val create_right = spark.sql(
            """
              |CREATE TEMPORARY view mirrior_right
              |USING org.elasticsearch.spark.sql
              |OPTIONS (resource 'mysearch-mirror-ads_iii_dev-v2/_doc',
              |          nodes '10.199.151.14', port '9200', net.http.auth.user 'mysearch',
              |          net.http.auth.pass 'mysearch_123')""".stripMargin)
        //al mirror2_frame = spark.sql("select `institution_type`,`@del`,`@mt`,`@id`,`id`,`institution_id` from mirrior_right")
        //mirror2_frame.collect().foreach(println(_))
        val create_result = spark.sql(
            """
              |CREATE TEMPORARY view scene(
              | `mcbbi_1@customer_type` string,
              | `mcbbi_1@@del`  string,
              | `mcbbi_1@@mt` timestamp,
              | `mcbbi_1@@id` string,
              | `mcbbi_1@id` long ,
              | `mcbbi_1@customer_id` string,
              | `aiii_2@institution_type` string,
              | `aiii_2@@del` string,
              | `aiii_2@@mt` timestamp,
              | `aiii_2@@id` string,
              | `aiii_2@id` long,
              | `aiii_2@institution_id` string,
              | `@timestamp` timestamp
              |)
              |USING org.elasticsearch.spark.sql
              |OPTIONS (resource 'mysearch-scene-multi_join_wsk_test-v6/_doc',
              |          nodes '10.199.151.14', port '9200', net.http.auth.user 'mysearch',
              |          net.http.auth.pass 'mysearch_123', es.mapping.id 'mcbbi_1@@id')""".stripMargin)

        spark.sql("select * from scene").printSchema()
        val join_frame = spark.sql(
            """
              |insert into scene
              |select l.`customer_type` as `mcbbi_1@customer_type`,
              |l.`@del` as `mcbbi_1@@del`,
              | now() as `mcbbi_1@@mt`,
              | l.`@id` as `mcbbi_1@@id`,
              | cast(l.`id` as bigint)  as `mcbbi_1@id`,
              | l.`customer_id` as `mcbbi_1@customer_id`,
              | r.`institution_type` as `aiii_2@institution_type`,
              | r.`@del`  as `aiii_2@@del`,
              | now() as `aiii_2@@mt`,
              | r.`@id` as `aiii_2@@id`,
              | cast(r.`id` as bigint ) as `aiii_2@id`,
              | r.`institution_id` as `aiii_2@institution_id`,
              | now()  as `@timestamp`
              | from mirrior_left l left join mirrior_right r
              |where  l.customer_id=r.institution_id and l.customer_type = r.institution_type and  l.id != 311
              |""".stripMargin)
//        join_frame.printSchema()
//        join_frame.collect().foreach(println(_))

        spark.stop()

    }

}
