package com.wsk.spark.jdbc

import scalikejdbc._
import scalikejdbc.config._

case class Employer(name: String, age: Int, salary: Long)

object JdbcTest {
  def main(args: Array[String]): Unit = {
    DBs.setupAll()
    val employers = List(Employer("zhangsan", 20, 18000), Employer("zhangliu", 50, 300000), Employer("lisi", 22, 22000))
    //批量插入
    insert(employers)
    println("----------------insert执行完毕---------------")
    //查询出结果
    val results = select()
    for (employer <- results) {
      println(employer.name, employer.age, employer.salary)
    }
    println("----------------select执行完毕---------------")
    //修改
    update(1000, "zhangsan")
    println("----------------update执行完毕---------------")
    //根据姓名删除
    deleteByname("zhangliu")
    println("----------------deleteByname执行完毕---------------")
    //删除所有记录
    deleteAll()
    println("----------------deleteAll执行完毕---------------")

    DBs.closeAll()
  }


  def insert(employers: List[Employer]): Unit = {
    DB.localTx { implicit session =>
      for (employer <- employers) {
        SQL("insert into wsktest(name,age,salary) values(?,?,?)")
          .bind(employer.name, employer.age, employer.salary)
          .update().apply()
      }

    }
  }

  def select(): List[Employer] = {
    DB.readOnly { implicit session =>
      SQL("select * from wsktest")
        .map(rs => Employer(rs.string("name"), rs.int("age"), rs.long("salary")))
        .list().apply()
    }
  }

  def update(age: Int, name: String) {
    DB.autoCommit { implicit session =>
      SQL("update wsktest set age = ? where name = ?").bind(age, name).update().apply()
    }
  }


  def deleteByname(name: String): Unit = {
    DB.autoCommit { implicit session =>
      SQL("delete from wsktest where name = ?").bind(name).update().apply()
    }
  }

  def deleteAll(): Unit ={
    DB.autoCommit { implicit session =>
      SQL("delete from wsktest ").update().apply()
    }
  }
}


