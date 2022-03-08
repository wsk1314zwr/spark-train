package com.wsk.spark.sentry

import org.apache.hadoop.hive.ql.plan.HiveOperation
import org.apache.spark.sql.catalyst.plans.logical.{Command, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.datasources.CreateTempViewUsing
import org.apache.spark.sql.hive.execution.CreateHiveTableAsSelectCommand
import org.apache.spark.sql.execution.datasources.InsertIntoDataSourceCommand
import org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand


class SentryAuthRule extends Rule[LogicalPlan]{
    override def apply(plan: LogicalPlan): LogicalPlan = {
        //权限认证

        plan
    }

    def getHiveOperation(plan: LogicalPlan): HiveOperation = {
        plan match {
            case c: Command => c match {
                case _: AlterDatabasePropertiesCommand => HiveOperation.ALTERDATABASE
                case p if p.nodeName == "AlterTableAddColumnsCommand" => HiveOperation.ALTERTABLE_ADDCOLS
                case _: AlterTableAddPartitionCommand => HiveOperation.ALTERTABLE_ADDPARTS
                case p if p.nodeName == "AlterTableChangeColumnCommand" =>
                    HiveOperation.ALTERTABLE_RENAMECOL
                case _: AlterTableDropPartitionCommand => HiveOperation.ALTERTABLE_DROPPARTS
                case _: AlterTableRecoverPartitionsCommand => HiveOperation.MSCK
                case _: AlterTableRenamePartitionCommand => HiveOperation.ALTERTABLE_RENAMEPART
                case a: AlterTableRenameCommand =>
                    if (!a.isView) HiveOperation.ALTERTABLE_RENAME else HiveOperation.ALTERVIEW_RENAME
                case _: AlterTableSetPropertiesCommand
                     | _: AlterTableUnsetPropertiesCommand => HiveOperation.ALTERTABLE_PROPERTIES
                case _: AlterTableSerDePropertiesCommand => HiveOperation.ALTERTABLE_SERDEPROPERTIES
                case _: AlterTableSetLocationCommand => HiveOperation.ALTERTABLE_LOCATION
                case _: AlterViewAsCommand => HiveOperation.QUERY
                // case _: AlterViewAsCommand => HiveOperation.ALTERVIEW_AS

                case _: AnalyzeColumnCommand => HiveOperation.QUERY
                // case _: AnalyzeTableCommand => HiveOperation.ANALYZE_TABLE
                // Hive treat AnalyzeTableCommand as QUERY, obey it.
                case _: AnalyzeTableCommand => HiveOperation.QUERY
                case p if p.nodeName == "AnalyzePartitionCommand" => HiveOperation.QUERY

                case _: CreateDatabaseCommand => HiveOperation.CREATEDATABASE
                case _: CreateDataSourceTableAsSelectCommand
                     | _: CreateHiveTableAsSelectCommand => HiveOperation.CREATETABLE_AS_SELECT
                case _: CreateFunctionCommand => HiveOperation.CREATEFUNCTION
                case _: CreateTableCommand
                     | _: CreateDataSourceTableCommand => HiveOperation.CREATETABLE
                case _: CreateTableLikeCommand => HiveOperation.CREATETABLE
                case _: CreateViewCommand
                     | _: CacheTableCommand
                     | _: CreateTempViewUsing => HiveOperation.CREATEVIEW
                case p if p.nodeName == "DescribeColumnCommand" => HiveOperation.DESCTABLE
                case _: DescribeDatabaseCommand => HiveOperation.DESCDATABASE
                case _: DescribeFunctionCommand => HiveOperation.DESCFUNCTION
                case _: DescribeTableCommand => HiveOperation.DESCTABLE
                case _: DropDatabaseCommand => HiveOperation.DROPDATABASE
                // Hive don't check privileges for `drop function command`, what about a unverified user
                // try to drop functions.
                // We treat permanent functions as tables for verifying.
                case d: DropFunctionCommand if !d.isTemp => HiveOperation.DROPTABLE
                case d: DropFunctionCommand if d.isTemp => HiveOperation.DROPFUNCTION
                case _: DropTableCommand => HiveOperation.DROPTABLE
                case e: ExplainCommand => getHiveOperation(e.logicalPlan)
                case _: InsertIntoDataSourceCommand => HiveOperation.QUERY
                case p if p.nodeName == "InsertIntoDataSourceDirCommand" => HiveOperation.QUERY
                //case _: InsertIntoHadoopFsRelationCommand => HiveOperation.CREATETABLE_AS_SELECT
                case _: InsertIntoHadoopFsRelationCommand => HiveOperation.QUERY
                case p if p.nodeName == "InsertIntoHiveDirCommand" => HiveOperation.QUERY
                case p if p.nodeName == "InsertIntoHiveTable" => HiveOperation.QUERY
                case _: LoadDataCommand => HiveOperation.LOAD
                case p if p.nodeName == "SaveIntoDataSourceCommand" => HiveOperation.QUERY
                case s: SetCommand if s.kv.isEmpty || s.kv.get._2.isEmpty => HiveOperation.SHOWCONF
//                case _: SetDatabaseCommand => HiveOperation.SWITCHDATABASE
                case _: ShowCreateTableCommand => HiveOperation.SHOW_CREATETABLE
                case _: ShowColumnsCommand => HiveOperation.SHOWCOLUMNS
//                case _: ShowDatabasesCommand => HiveOperation.SHOWDATABASES
                case _: ShowFunctionsCommand => HiveOperation.SHOWFUNCTIONS
                case _: ShowPartitionsCommand => HiveOperation.SHOWPARTITIONS
                case _: ShowTablesCommand => HiveOperation.SHOWTABLES
                case _: ShowTablePropertiesCommand => HiveOperation.SHOW_TBLPROPERTIES
                case s: StreamingExplainCommand =>
                    getHiveOperation(s.queryExecution.optimizedPlan)
                case _: TruncateTableCommand => HiveOperation.TRUNCATETABLE
                case _: UncacheTableCommand => HiveOperation.DROPVIEW
                // Commands that do not need build privilege goes as explain type
                case _ =>
                    // AddFileCommand
                    // AddJarCommand
                    // SetDatabaseCommand 没找到对应的spark
                    //ShowDatabasesCommand
                    HiveOperation.EXPLAIN
            }
            case _ => HiveOperation.QUERY
        }
    }

}
