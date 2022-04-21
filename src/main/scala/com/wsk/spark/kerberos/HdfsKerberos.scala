package com.wsk.spark.kerberos

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.security.UserGroupInformation

object HdfsKerberos {

  /**
    * 进行hdfs kerberos 登陆验证
    */
  def hdfsKerberos(): Unit ={
    val conf = new Configuration()
    kerberos(conf)

    //需要添加如下两个文件才可访问hdfs，光加HADOOP_HOME以及HADOOP_CONF_DIR是没有用的，有这两个文件后可不需要HADOOP_HOME以及HADOOP_CONF_DIR的配置
    conf.addResource(new Path("/opt/env/core-site.xml"))
    conf.addResource(new Path("/opt/env/hdfs-site.xml"))
    //访问hdfs代码
    val fs = FileSystem.get(conf)
    fs.globStatus(new Path("/spark/spark3/jars/*")).map(x=>{
      println(x.getPath)
    })

  }

  /**
   * kerberos 登陆验证
   */
  def kerberos(conf: Configuration): Unit ={

    val user = "hive/hz-hadoop-test-199-151-39@HADOOP.COM"
    val keyPath = "/opt/env/hive.keytab"
    val krb5Path = "/opt/env/krb5.conf"

    System.setProperty("java.security.krb5.conf", krb5Path)

    conf.set("hadoop.security.authentication", "kerberos")
    UserGroupInformation.setConfiguration(conf)
    UserGroupInformation.loginUserFromKeytab(user,keyPath)

  }

  def main(args: Array[String]): Unit = {
    hdfsKerberos()
  }
}
