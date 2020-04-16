package com.wsk.spark.kudu

import org.apache.kudu.{ColumnSchema, Schema, Type}
import org.apache.kudu.client.{AlterTableOptions, CreateTableOptions, KuduClient}

import scala.collection.JavaConverters._
import scala.collection.mutable
/**
  * 讲师：若泽(PK哥)
  * 交流群：126181630
  */
object KuduAPIApp {


  def createTable(client: KuduClient, tableName: String): Unit = {

    val columns = List(
      new ColumnSchema.ColumnSchemaBuilder("word", Type.STRING).key(true).build(),
      new ColumnSchema.ColumnSchemaBuilder("cnt", Type.INT32).build()
    ).asJava

    val schema = new Schema(columns)

    val options = new CreateTableOptions()
    options.setNumReplicas(1)
    val list = new java.util.LinkedList[String]()
    list.add("word")
    options.addHashPartitions(list, 3)

    client.createTable(tableName, schema, options)
  }

  def insertRows(client: KuduClient, tableName: String): Unit = {
    val table = client.openTable(tableName)
    val session = client.newSession()

    for(i<-1 to 10) {
      val insert = table.newInsert()
      val row = insert.getRow
      row.addString("word", s"pk-$i")
      row.addInt("cnt", 1000+i)
      session.apply(insert)
    }

    session.close()
  }

  def query(client: KuduClient, tableName: String): Unit = {
    val table = client.openTable(tableName)
    val scanner = client.newScannerBuilder(table).build()
    while(scanner.hasMoreRows){
      val iterator = scanner.nextRows()
      while(iterator.hasNext) {
        val result = iterator.next()
        println(result.getString("word") + " ==> " + result.getInt("cnt"))
      }
    }
  }

  def alterRow(client: KuduClient, tabkeName: String): Unit = {
    val table = client.openTable(tabkeName)
    val session = client.newSession()

    val update = table.newUpdate()
    val row = update.getRow
    row.addString("word","pk-10")
    row.addInt("cnt",8888)
    session.apply(update)

    session.close()
  }

  def renameTable(client: KuduClient, tableName: String,
                  newTableName: String) = {

    val options = new AlterTableOptions()
    options.renameTable(newTableName)
    client.alterTable(tableName,options)
  }

  def deleteTable(client: KuduClient, tableName: String): Unit = {
    client.deleteTable(tableName)
  }

  def main(args: Array[String]): Unit = {
    val KUDU_MASTERS = "hadoop000"
    val tableName = "g6_pk"
    val client = new KuduClient.KuduClientBuilder(KUDU_MASTERS).build()

//    createTable(client:KuduClient, tableName:String)

    //insertRows(client:KuduClient, tableName:String)

//    val newTableName = "new_g6_pk"
//    renameTable(client, tableName,newTableName)
//
    query(client:KuduClient, tableName:String)

//    alterRow(client:KuduClient, tableName:String)

//    deleteTable(client, newTableName)
  }

}
