//package com.wsk.spark.sentry
//
//import org.apache.hadoop.hive.ql.plan.HiveOperation
//import org.apache.spark.sql.catalyst.plans.logical.{Command, LogicalPlan, Project}
//import org.apache.spark.sql.catalyst.rules.Rule
//import org.apache.spark.sql.execution.command._
//import org.apache.spark.sql.execution.datasources.CreateTempViewUsing
//import org.apache.spark.sql.execution.datasources.InsertIntoDataSourceCommand
//import org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand
//import java.util
//import org.apache.commons.lang3.StringUtils
//import org.apache.hadoop.hive.ql.hooks.{ReadEntity, WriteEntity}
//import org.apache.hadoop.hive.metastore.api.Database
//import org.apache.hadoop.hive.ql.metadata.{AuthorizationException, Table}
//import org.apache.hadoop.security.UserGroupInformation
//import org.apache.spark.internal.Logging
//import org.apache.spark.sql.catalyst.TableIdentifier
//import org.apache.spark.sql.catalyst.expressions.NamedExpression
//import org.apache.spark.sql.hive.execution.CreateHiveTableAsSelectCommand
//
///**
// * sentry权限集成spark sql失败
// */
//class SentryAuthRule extends Rule[LogicalPlan] with Logging{
//    def sparkAuth:SparkAuthHook = new SparkAuthHook
//    override def apply(plan: LogicalPlan): LogicalPlan = {
//        try {
//            val hiveOperation = getHiveOperation(plan)
//            val inputs = new util.HashSet[ReadEntity]()
//            val outputs = new util.HashSet[WriteEntity]()
//            plan match {
//                // RunnableCommand
//                case cmd: Command => buildCommand(cmd, inputs, outputs)
//                // Queries
//                case _ => buildQuery(plan, inputs)
//            }
//            val user = UserGroupInformation.getCurrentUser.getShortUserName
//            sparkAuth.auth(hiveOperation, inputs, outputs, user);
//        } catch {
//            case e: AuthorizationException =>
//                val lastQueryPrivilegeErrors = sparkAuth.getHiveAuthzBinding.getLastQueryPrivilegeErrors
//                val errors = StringUtils.join(lastQueryPrivilegeErrors, ";")
//                logError(
//                    s"""
//             ${e.getMessage}
//             The required privileges: ${errors}
//               """.stripMargin)
//                throw e
//            case e: Exception =>
//        }
//        plan
//    }
//    private[this] def buildCommand(plan: Command, inputs: util.HashSet[ReadEntity], outputs: util.HashSet[WriteEntity]): Unit = {
//        def addTableOrViewLevelObjs4Input(table: TableIdentifier): Unit = {
//            table.database match {
//                case Some(db) =>
//                    val tbName = table.table
//                    inputs.add(new ReadEntity(new Table(db, tbName)))
//                case _ =>
//            }
//        }
//        def addTableOrViewLevelObjs4Output(table: TableIdentifier, writeType: WriteEntity.WriteType = WriteEntity.WriteType.DDL_SHARED): Unit = {
//            table.database match {
//                case Some(db) =>
//                    val tbName = table.table
//                    outputs.add(new WriteEntity(new Table(db, tbName), writeType))
//                case _ =>
//            }
//        }
//        def addDbLevelObjs4Input(databaseName: String): Unit = {
//            // TODO 确认操作
//            val database = new Database()
//            database.setName(databaseName)
//            inputs.add(new ReadEntity(database))
//        }
//        def addDbLevelObjs4Output(databaseName: String, writeType: WriteEntity.WriteType = WriteEntity.WriteType.DDL_SHARED): Unit = {
//            // TODO 确认操作
//            val database = new Database()
//            database.setName(databaseName)
//            outputs.add(new WriteEntity(database, writeType))
//        }
//        plan match {
//            case a: AlterDatabasePropertiesCommand =>
//                addDbLevelObjs4Output(a.databaseName)
//            case a if a.nodeName == "AlterTableAddColumnsCommand" =>
//                // TODO 给表添加列
//                addTableOrViewLevelObjs4Input(getFieldVal(a, "table").asInstanceOf[TableIdentifier])
//                addTableOrViewLevelObjs4Output(getFieldVal(a, "table").asInstanceOf[TableIdentifier])
//            case a: AlterTableAddPartitionCommand =>
//                // TODO 添加表分区
//                addTableOrViewLevelObjs4Input(a.tableName)
//                addTableOrViewLevelObjs4Output(a.tableName)
//            case a if a.nodeName == "AlterTableChangeColumnCommand" =>
//                addTableOrViewLevelObjs4Input(getFieldVal(a, "tableName").asInstanceOf[TableIdentifier])
//            case a: AlterTableDropPartitionCommand =>
//                addTableOrViewLevelObjs4Input(a.tableName)
//                addTableOrViewLevelObjs4Output(a.tableName)
//            case a: AlterTableRecoverPartitionsCommand =>
//                addTableOrViewLevelObjs4Input(a.tableName)
//                addTableOrViewLevelObjs4Output(a.tableName)
//            case a: AlterTableRenameCommand if !a.isView || a.oldName.database.nonEmpty =>
//                // rename tables / permanent views
//                addTableOrViewLevelObjs4Input(a.oldName)
//                addTableOrViewLevelObjs4Output(a.newName)
//            case a: AlterTableRenamePartitionCommand =>
//                addTableOrViewLevelObjs4Input(a.tableName)
//                addTableOrViewLevelObjs4Output(a.tableName)
//            case a: AlterTableSerDePropertiesCommand =>
//                addTableOrViewLevelObjs4Input(a.tableName)
//                addTableOrViewLevelObjs4Output(a.tableName)
//            case a: AlterTableSetLocationCommand =>
//                addTableOrViewLevelObjs4Input(a.tableName)
//                addTableOrViewLevelObjs4Output(a.tableName)
//            case a: AlterTableSetPropertiesCommand =>
//                addTableOrViewLevelObjs4Input(a.tableName)
//                addTableOrViewLevelObjs4Output(a.tableName)
//            case a: AlterTableUnsetPropertiesCommand =>
//                addTableOrViewLevelObjs4Input(a.tableName)
//                addTableOrViewLevelObjs4Output(a.tableName)
//            case a: AlterViewAsCommand =>
//                if (a.name.database.nonEmpty) {
//                    // it's a permanent view
//                    addTableOrViewLevelObjs4Output(a.name)
//                }
//                buildQuery(a.query, inputs)
//            case a: AnalyzeColumnCommand =>
//                addTableOrViewLevelObjs4Input(a.tableIdent)
//                addTableOrViewLevelObjs4Output(a.tableIdent)
//            case a if a.nodeName == "AnalyzePartitionCommand" =>
//                addTableOrViewLevelObjs4Input(getFieldVal(a, "tableIdent").asInstanceOf[TableIdentifier])
//                addTableOrViewLevelObjs4Output(getFieldVal(a, "tableIdent").asInstanceOf[TableIdentifier])
//            case a: AnalyzeTableCommand =>
//                addTableOrViewLevelObjs4Input(getFieldVal(a, "tableIdent").asInstanceOf[TableIdentifier])
//                addTableOrViewLevelObjs4Output(getFieldVal(a, "tableIdent").asInstanceOf[TableIdentifier])
//            case c: CacheTableCommand => c.plan.foreach {
//                buildQuery(_, inputs)
//            }
//            case c: CreateDatabaseCommand => addDbLevelObjs4Output(c.databaseName)
//            case c: CreateDataSourceTableAsSelectCommand =>
//                c.table.identifier.database match {
//                    case Some(db) =>
//                        addDbLevelObjs4Output(db)
//                    case _ =>
//                }
//                addTableOrViewLevelObjs4Output(c.table.identifier)
//                buildQuery(c.query, inputs)
//            case c: CreateDataSourceTableCommand =>
//                addTableOrViewLevelObjs4Output(c.table.identifier)
//            case c: CreateFunctionCommand if !c.isTemp =>
//                addDbLevelObjs4Output(c.databaseName.get)
//            //TODO function 不考虑先
//            //addFunctionLevelObjs(c.databaseName, c.functionName, outputObjs)
//            case c: CreateHiveTableAsSelectCommand =>
//                c.tableDesc.identifier.database match {
//                    case Some(db) =>
//                        addDbLevelObjs4Output(db)
//                    case _ =>
//                }
//                addTableOrViewLevelObjs4Output(c.tableDesc.identifier)
//                buildQuery(c.query, inputs)
//            case c: CreateTableCommand =>
//                // 如果创建表的话需要库的权限
//                addDbLevelObjs4Output(c.table.identifier.database.get, WriteEntity.WriteType.DDL_SHARED)
//            case c: CreateTableLikeCommand =>
//                c.targetTable.database match {
//                    case Some(db) =>
//                        addDbLevelObjs4Output(db)
//                    case _ =>
//                }
//                // hive don't handle source table's privileges, we should not obey that, because
//                // it will cause meta information leak
//                addTableOrViewLevelObjs4Input(c.sourceTable)
//            case c: CreateViewCommand =>
//                c.viewType match {
//                    case PersistedView =>
//                        // PersistedView will be tied to a database
//                        c.name.database match {
//                            case Some(db) =>
//                                addDbLevelObjs4Output(db)
//                            case _ =>
//                        }
//                        addTableOrViewLevelObjs4Output(c.name)
//                    case _ =>
//                }
//                buildQuery(c.child, inputs)
//            case d if d.nodeName == "DescribeColumnCommand" =>
//                addTableOrViewLevelObjs4Input(getFieldVal(d, "table").asInstanceOf[TableIdentifier])
//            case d: DescribeDatabaseCommand =>
//                addDbLevelObjs4Input(d.databaseName)
//            case d: DescribeFunctionCommand =>
//            // todo
//            // addFunctionLevelObjs(d.functionName.database, d.functionName.funcName, inputObjs)
//            case d: DescribeTableCommand => addTableOrViewLevelObjs4Input(d.table)
//            case d: DropDatabaseCommand =>
//                // outputObjs are enough for privilege check, adding inputObjs for consistency with hive
//                // behaviour in case of some unexpected issues.
//                addDbLevelObjs4Input(d.databaseName)
//                addDbLevelObjs4Output(d.databaseName)
//            case d: DropFunctionCommand =>
//                addDbLevelObjs4Output(d.databaseName.get)
//            case d: DropTableCommand => addTableOrViewLevelObjs4Input(d.tableName)
//            case i: InsertIntoDataSourceCommand =>
//                i.logicalRelation.catalogTable.foreach { table =>
//                    addTableOrViewLevelObjs4Output(table.identifier)
//                }
//                buildQuery(i.query, inputs)
//            case i if i.nodeName == "InsertIntoDataSourceDirCommand" =>
//                buildQuery(getFieldVal(i, "query").asInstanceOf[LogicalPlan], inputs)
//            case i: InsertIntoHadoopFsRelationCommand =>
//                // we are able to get the override mode here, but ctas for hive table with text/orc
//                // format and parquet with spark.sql.hive.convertMetastoreParquet=false can success
//                // with privilege checking without claiming for UPDATE privilege of target table,
//                // which seems to be same with Hive behaviour.
//                // So, here we ignore the overwrite mode for such a consistency.
//                i.catalogTable foreach { t =>
//                    addTableOrViewLevelObjs4Output(t.identifier)
//                }
//                buildQuery(i.query, inputs)
//            case i if i.nodeName == "InsertIntoHiveDirCommand" =>
//                buildQuery(getFieldVal(i, "query").asInstanceOf[LogicalPlan], inputs)
//            case i if i.nodeName == "InsertIntoHiveTable" =>
//                addTableOrViewLevelObjs4Output(getFieldVal(i, "table").asInstanceOf[CatalogTable].identifier, WriteEntity.WriteType.INSERT)
//                buildQuery(getFieldVal(i, "query").asInstanceOf[LogicalPlan], inputs)
//            case l: LoadDataCommand =>
//                addTableOrViewLevelObjs4Output(l.table)
//            case s if s.nodeName == "SaveIntoDataSourceCommand" =>
//            // TODO
//            // buildQuery(getFieldVal(s, "query").asInstanceOf[LogicalPlan], outputObjs)
//            case s: SetDatabaseCommand =>
//                // 切换数据库
//                addDbLevelObjs4Input(s.databaseName)
//            //        addDbLevelObjs4Output(s.databaseName)
//            case s: ShowColumnsCommand => addTableOrViewLevelObjs4Input(s.tableName)
//            case s: ShowCreateTableCommand => addTableOrViewLevelObjs4Input(s.table)
//            case s: ShowFunctionsCommand => s.db.foreach(addDbLevelObjs4Input(_))
//            case s: ShowPartitionsCommand => addTableOrViewLevelObjs4Input(s.tableName)
//            case s: ShowTablePropertiesCommand => addTableOrViewLevelObjs4Input(s.table)
//            case s: ShowTablesCommand => addDbLevelObjs4Input(s.databaseName.get)
//            case s: TruncateTableCommand =>
//                addTableOrViewLevelObjs4Output(s.tableName)
//            case _ =>
//        }
//    }
//
//    private[this] def buildQuery(plan: LogicalPlan, inputs: util.HashSet[ReadEntity], projectionList: Seq[NamedExpression] = Nil): Unit = {
//        def addInput(table: TableIdentifier): Unit = {
//            table.database match {
//                case Some(db) =>
//                    val tbName = table.table
//                    inputs.add(new ReadEntity(new Table(db, tbName)))
//                case _ =>
//            }
//        }
//        plan match {
//            case p: Project => buildQuery(p.child, inputs, p.projectList)
//            case h if h.nodeName == "HiveTableRelation" =>
//                addInput(getFieldVal(h, "tableMeta").asInstanceOf[CatalogTable].identifier)
//            case m if m.nodeName == "MetastoreRelation" =>
//                addInput(getFieldVal(m, "catalogTable").asInstanceOf[CatalogTable].identifier)
//            case c if c.nodeName == "CatalogRelation" =>
//                addInput(getFieldVal(c, "tableMeta").asInstanceOf[CatalogTable].identifier)
//            case l: LogicalRelation if l.catalogTable.nonEmpty => addInput(l.catalogTable.get.identifier)
//            case u: UnresolvedRelation =>
//                // Normally, we shouldn't meet UnresolvedRelation here in an optimized plan.
//                // Unfortunately, the real world is always a place where miracles happen.
//                // We check the privileges directly without resolving the plan and leave everything
//                // to spark to do.
//                addInput(u.tableIdentifier)
//            case p =>
//                for (child <- p.children) {
//                    buildQuery(child, inputs, projectionList)
//                }
//        }
//    }
//
//
//    def getHiveOperation(plan: LogicalPlan): HiveOperation = {
//        plan match {
//            case c: Command => c match {
//                case _: AlterDatabasePropertiesCommand => HiveOperation.ALTERDATABASE
//                case p if p.nodeName == "AlterTableAddColumnsCommand" => HiveOperation.ALTERTABLE_ADDCOLS
//                case _: AlterTableAddPartitionCommand => HiveOperation.ALTERTABLE_ADDPARTS
//                case p if p.nodeName == "AlterTableChangeColumnCommand" =>
//                    HiveOperation.ALTERTABLE_RENAMECOL
//                case _: AlterTableDropPartitionCommand => HiveOperation.ALTERTABLE_DROPPARTS
//                case _: AlterTableRecoverPartitionsCommand => HiveOperation.MSCK
//                case _: AlterTableRenamePartitionCommand => HiveOperation.ALTERTABLE_RENAMEPART
//                case a: AlterTableRenameCommand =>
//                    if (!a.isView) HiveOperation.ALTERTABLE_RENAME else HiveOperation.ALTERVIEW_RENAME
//                case _: AlterTableSetPropertiesCommand
//                     | _: AlterTableUnsetPropertiesCommand => HiveOperation.ALTERTABLE_PROPERTIES
//                case _: AlterTableSerDePropertiesCommand => HiveOperation.ALTERTABLE_SERDEPROPERTIES
//                case _: AlterTableSetLocationCommand => HiveOperation.ALTERTABLE_LOCATION
//                case _: AlterViewAsCommand => HiveOperation.QUERY
//                // case _: AlterViewAsCommand => HiveOperation.ALTERVIEW_AS
//
//                case _: AnalyzeColumnCommand => HiveOperation.QUERY
//                // case _: AnalyzeTableCommand => HiveOperation.ANALYZE_TABLE
//                // Hive treat AnalyzeTableCommand as QUERY, obey it.
//                case _: AnalyzeTableCommand => HiveOperation.QUERY
//                case p if p.nodeName == "AnalyzePartitionCommand" => HiveOperation.QUERY
//
//                case _: CreateDatabaseCommand => HiveOperation.CREATEDATABASE
//                case _: CreateDataSourceTableAsSelectCommand
//                     | _: CreateHiveTableAsSelectCommand => HiveOperation.CREATETABLE_AS_SELECT
//                case _: CreateFunctionCommand => HiveOperation.CREATEFUNCTION
//                case _: CreateTableCommand
//                     | _: CreateDataSourceTableCommand => HiveOperation.CREATETABLE
//                case _: CreateTableLikeCommand => HiveOperation.CREATETABLE
//                case _: CreateViewCommand
//                     | _: CacheTableCommand
//                     | _: CreateTempViewUsing => HiveOperation.CREATEVIEW
//                case p if p.nodeName == "DescribeColumnCommand" => HiveOperation.DESCTABLE
//                case _: DescribeDatabaseCommand => HiveOperation.DESCDATABASE
//                case _: DescribeFunctionCommand => HiveOperation.DESCFUNCTION
//                case _: DescribeTableCommand => HiveOperation.DESCTABLE
//                case _: DropDatabaseCommand => HiveOperation.DROPDATABASE
//                // Hive don't check privileges for `drop function command`, what about a unverified user
//                // try to drop functions.
//                // We treat permanent functions as tables for verifying.
//                case d: DropFunctionCommand if !d.isTemp => HiveOperation.DROPTABLE
//                case d: DropFunctionCommand if d.isTemp => HiveOperation.DROPFUNCTION
//                case _: DropTableCommand => HiveOperation.DROPTABLE
//                case e: ExplainCommand => getHiveOperation(e.logicalPlan)
//                case _: InsertIntoDataSourceCommand => HiveOperation.QUERY
//                case p if p.nodeName == "InsertIntoDataSourceDirCommand" => HiveOperation.QUERY
//                //case _: InsertIntoHadoopFsRelationCommand => HiveOperation.CREATETABLE_AS_SELECT
//                case _: InsertIntoHadoopFsRelationCommand => HiveOperation.QUERY
//                case p if p.nodeName == "InsertIntoHiveDirCommand" => HiveOperation.QUERY
//                case p if p.nodeName == "InsertIntoHiveTable" => HiveOperation.QUERY
//                case _: LoadDataCommand => HiveOperation.LOAD
//                case p if p.nodeName == "SaveIntoDataSourceCommand" => HiveOperation.QUERY
//                case s: SetCommand if s.kv.isEmpty || s.kv.get._2.isEmpty => HiveOperation.SHOWCONF
////                case _: SetDatabaseCommand => HiveOperation.SWITCHDATABASE
//                case _: ShowCreateTableCommand => HiveOperation.SHOW_CREATETABLE
//                case _: ShowColumnsCommand => HiveOperation.SHOWCOLUMNS
////                case _: ShowDatabasesCommand => HiveOperation.SHOWDATABASES
//                case _: ShowFunctionsCommand => HiveOperation.SHOWFUNCTIONS
//                case _: ShowPartitionsCommand => HiveOperation.SHOWPARTITIONS
//                case _: ShowTablesCommand => HiveOperation.SHOWTABLES
//                case _: ShowTablePropertiesCommand => HiveOperation.SHOW_TBLPROPERTIES
//                case s: StreamingExplainCommand =>
//                    getHiveOperation(s.queryExecution.optimizedPlan)
//                case _: TruncateTableCommand => HiveOperation.TRUNCATETABLE
//                case _: UncacheTableCommand => HiveOperation.DROPVIEW
//                // Commands that do not need build privilege goes as explain type
//                case _ =>
//                    // AddFileCommand
//                    // AddJarCommand
//                    // SetDatabaseCommand spark3 中没找到对应的SetDatabaseCommand类
//                    // ShowDatabasesCommand spark3 中没找到对应的ShowDatabasesCommand类
//                    HiveOperation.EXPLAIN
//            }
//            case _ => HiveOperation.QUERY
//        }
//    }
//
//}
