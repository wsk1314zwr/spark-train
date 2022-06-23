package com.wsk.spark.sql.hvie

import com.wsk.spark.kerberos.HdfsKerberos
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.{Row, SparkSession}

/**
 * 操作远程的hive数仓
 *
 * 1）需要移除sentry的pom依赖，并放开spark-hive_2.12依赖的相关的hive包
 * 2）添加对应数仓的hive-site.xml(提供了是通过jdbc链接metasore数据库库或通过thrift连接metastore服务读写hive metastore)文件
 * 3）添加core-site.xml和hdfs-site.xml,这两个文件指明存放hive数据的hadoop的部署信息，不然无法读写hive数据
 * 4）添加spark.sql.hive想关的配置，指明hive的版本以及hive的相关jar包路径，不然会因为代码冲突报异常
 * 5) enableHiveSupport(),开启支持hive数仓
 * 5）若hive开启了kerberos，idea调测需要进行kerberos登录认证
 * 6) 若hive开启了kerberos，本机添加hive用户以及用户组信息，因为插入等操作，会校验权限，校验权限的第一步是获取本机的hive用户以及组的信息，无用户信息会报异常
 * 7) 添加yarn-site.xml，不然spark读取其自定义的表数据会报kerberos错误，但是加了会导致其它非需要kerberos任务(操作es)任务报错，需要移走，放在了resources/config下
 * 8）需将pom的hadooop依赖设置为hadoop3，不然会有些hadoop类找不到
 */

object SparkHiveRemoteExample {


    // $example on:spark_hive$
    case class Record(key: Int, value: String)
    // $example off:spark_hive$

    def main(args: Array[String]) {
        // When working with Hive, one must instantiate `SparkSession` with Hive support, including
        // connectivity to a persistent Hive metastore, support for Hive serdes, and Hive user-defined
        // functions. Users who do not have an existing Hive deployment can still enable Hive support.
        // When not configured by the hive-site.xml, the context automatically creates `metastore_db`
        // in the current directory and creates a directory configured by `spark.sql.warehouse.dir`,
        // which defaults to the directory `spark-warehouse` in the current directory that the spark
        // application is started.

        // $example on:spark_hive$
        // warehouseLocation points to the default location for managed databases and tables
        HdfsKerberos.kerberos(new Configuration())
        val spark = SparkSession
                .builder()
                .master("local[2]")
                .appName("Spark Hive Example")
                .config("spark.sql.hive.metastore.version", "1.1.0")
                .config("spark.sql.hive.metastore.jars", "path")
                .config("spark.sql.hive.metastore.jars.path", "file:///opt/cloudera/parcels/CDH-5.16.1-1.cdh5.16.1.p0.3/lib/hive/lib/*")
                .enableHiveSupport()
                .getOrCreate()
        import spark.implicits._
        import spark.sql
//        spark.sql("show databases").collect().foreach(println(_))
//        spark.sql("insert into hive_test.wsk_pt_m_lifecycle_test7 PARTITION(pt_d = '2021-01-01') select 1").collect().foreach(println(_))
//        spark.sql("select count(1) from hive_test.wsk_pt_m_lifecycle_test7  ").collect().foreach(println(_))


        //测试mvel表达式
//        spark.sql("create temporary function esprocess as 'cn.com.servyou.pubdata.datark.udf.EsProcessUDF' ")
//        val frame1 = spark.sql("select esProcess('1+1','{}')")
//        frame1.collect().foreach(println(_))
//        val frame2 = spark.sql("select esProcess('cn.com.servyou.mysearch.core.process.processor.MVELFunctionUtils.vaultDecrypt(\"c20e1f58-5711-4900-882f-b154ad481d44\",\"xqy-nt-tax\",\"XQeUQJzI/HBIV4/rK5J/bXmXbH05aKRWuZjgGV3bGSA=\",this[\\'mcbbi_3@operator\\'])','{\"mcbbi_3@operator\":\"TcOzrEksqCSyB6Kmjh9BL6dKKoJPZ4mkJbunQt0xpuk=\"}');")
//        String s =                                        "cn.com.servyou.mysearch.core.process.processor.MVELFunctionUtils.vaultDecrypt(\"c20e1f58-5711-4900-882f-b154ad481d44\",\"xqy-nt-tax\",\"XQeUQJzI/HBIV4/rK5J/bXmXbH05aKRWuZjgGV3bGSA=\",?this[\\'mcbbi_3@operator\\']',string(mcbbi_3.`@data`)"
//        frame2.collect().foreach(println(_))
//        spark.sql("CREATE TEMPORARY VIEW mcbbi_3 USING org.elasticsearch.spark.sql OPTIONS (\n  resource 'mysearch-mirror-md_cbb_info_dev-v5/_doc',\n  nodes '10.199.151.14',\n  port '9200',\n  net.http.auth.user 'mysearch',\n  net.http.auth.pass 'mysearch_123'\n); ")
//        val frame = spark.sql("select esProcess('cn.com.servyou.mysearch.core.process.processor.MVELFunctionUtils.vaultDecrypt(\"c20e1f58-5711-4900-882f-b154ad481d44\",\"xqy-nt-tax\",\"XQeUQJzI/HBIV4/rK5J/bXmXbH05aKRWuZjgGV3bGSA=\",this[\\'operator\\'])',string(mcbbi_3.`@data`)) from mcbbi_3 mcbbi_3;")
//        frame.collect().foreach(print(_))

        sql("CREATE TABLE IF NOT EXISTS hive_test.wsk_test20220321 (key INT, value STRING) USING hive")
//        sql("LOAD DATA LOCAL INPATH '/Users/skwang/Documents/workspace/workspace4/project/my_project/spark-train/exampleData/kv1.txt' INTO TABLE hive_test.wsk_test20220321")

        // Queries are expressed in HiveQL
        sql("SELECT * FROM hive_test.wsk_pt_m_lifecycle_test7 ").collect().foreach(println(_))
        sql("SELECT * FROM hive_test.wsk_test20220321 ").show(10)
        // +---+-------+
        // |key|  value|
        // +---+-------+
        // |238|val_238|
        // | 86| val_86|
        // |311|val_311|
        // ...

        // Aggregation queries are also supported.
        sql("SELECT COUNT(*) FROM hive_test.wsk_test20220321 ").show()
        // +--------+
        // |count(1)|
        // +--------+
        // |    500 |
        // +--------+

        // The results of SQL queries are themselves DataFrames and support all normal functions.
        val sqlDF = sql("SELECT key, value FROM hive_test.wsk_test20220321  WHERE key < 10 ORDER BY key")

        // The items in DataFrames are of type Row, which allows you to access each column by ordinal.
        val stringsDS = sqlDF.map {
            case Row(key: Int, value: String) => s"Key: $key, Value: $value"
        }
        stringsDS.show()
        // +--------------------+
        // |               value|
        // +--------------------+
        // |Key: 0, Value: val_0|
        // |Key: 0, Value: val_0|
        // |Key: 0, Value: val_0|
        // ...

        // You can also use DataFrames to create temporary views within a SparkSession.
        val recordsDF = spark.createDataFrame((1 to 100).map(i => Record(i, s"val_$i")))
        recordsDF.createOrReplaceTempView("records")

        // Queries can then join DataFrame data with data stored in Hive.
        sql("SELECT * FROM records r JOIN hive_test.wsk_test20220321  s ON r.key = s.key").show()
        // +---+------+---+------+
        // |key| value|key| value|
        // +---+------+---+------+
        // |  2| val_2|  2| val_2|
        // |  4| val_4|  4| val_4|
        // |  5| val_5|  5| val_5|
        // ...
        // $example off:spark_hive$

        spark.stop()
    }

}
