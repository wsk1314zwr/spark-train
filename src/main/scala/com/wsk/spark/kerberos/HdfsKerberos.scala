package com.wsk.spark.kerberos

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.security.UserGroupInformation

class HdfsKerberos {

  /**
    * 进行hdfs kerberos 登陆验证
    */
  def hdfsKerberos(): Unit ={
    val conf = new Configuration()
    val user = "wsk@HADOOP.COM"
    val keyPath = "src/main/resources/cdh/wsk.keytab"
    val krb5Path = "src/main/resources/cdh/krb5.conf"

    //加载xml文件
    conf.addResource(new Path("src/main/resources/cdh/core-site.xml"))
    conf.addResource(new Path("src/main/resources/cdh/hdfs-site.xml"))
    //配置参数
    conf.set("hadoop.security.authentication", "kerberos")

    System.setProperty("java.security.krb5.conf", krb5Path)
    UserGroupInformation.setConfiguration(conf)
    UserGroupInformation.loginUserFromKeytab(user,keyPath)

    val fs = FileSystem.get(conf)
    fs.globStatus(new Path("/user/wsk*")).map(x=>{
      println(x.getLen)
    })

  }
}
