/*
 * Copyright (C) 2021 The Flowman Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dimajix.flowman.spec.relation

import scala.util.control.NonFatal

import io.delta.tables.DeltaTable
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.delta.commands.AlterTableAddColumnsDeltaCommand
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.MetadataBuilder
import org.apache.spark.sql.types.StructType

import com.dimajix.common.SetIgnoreCase
import com.dimajix.flowman.catalog.PartitionChange
import com.dimajix.flowman.catalog.PartitionSpec
import com.dimajix.flowman.catalog.TableChange
import com.dimajix.flowman.catalog.TableChange.AddColumn
import com.dimajix.flowman.catalog.TableChange.DropColumn
import com.dimajix.flowman.catalog.TableChange.UpdateColumnComment
import com.dimajix.flowman.catalog.TableChange.UpdateColumnNullability
import com.dimajix.flowman.catalog.TableChange.UpdateColumnType
import com.dimajix.flowman.catalog.TableDefinition
import com.dimajix.flowman.catalog.TableIdentifier
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.execution.SparkBackend
import com.dimajix.flowman.execution.MergeClause
import com.dimajix.flowman.execution.InsertClause
import com.dimajix.flowman.execution.UpdateClause
import com.dimajix.flowman.execution.DeleteClause
import com.dimajix.flowman.execution.MigrationFailedException
import com.dimajix.flowman.execution.MigrationStrategy
import com.dimajix.flowman.execution.OutputMode
import com.dimajix.flowman.jdbc.HiveDialect
import com.dimajix.flowman.model.BaseRelation
import com.dimajix.flowman.model.MigratableRelation
import com.dimajix.flowman.model.PartitionedRelation
import com.dimajix.flowman.model.ResourceIdentifier
import com.dimajix.flowman.model.SchemaRelation
import com.dimajix.flowman.spark.sql.delta.AlterTableChangeColumnDeltaCommand
import com.dimajix.flowman.spark.sql.delta.DeltaShim
import com.dimajix.flowman.spark.sql.delta.QualifiedColumn
import com.dimajix.spark.sql.DataFrameUtils


abstract class DeltaRelation(options: Map[String,String], mergeKey: Seq[String]) extends BaseRelation with PartitionedRelation with SchemaRelation with MigratableRelation {
    protected val resource : ResourceIdentifier

    /**
      * Returns the Delta table via the public io.delta.tables API. This is only usable in classic mode; in Connect
      * mode io.delta.tables reaches into the driver session state and the relation must run server-side SQL instead.
      */
    protected def deltaCatalogTable(execution: Execution) : DeltaTableV2
    protected def deltaTable(execution: Execution) : DeltaTable

    /**
      * Returns the current schema of the Delta table.
      */
    protected def deltaSchema(execution: Execution) : StructType

    /**
      * Returns the Delta table as a SQL identifier, either a (qualified) catalog table name or - for by-path tables -
      * a `delta.`<path>`` reference. This identifier is used to run ALTER/MERGE/DELETE statements against the Delta
      * table, which is how all DDL and DML is executed in Connect mode.
      */
    protected def sqlIdentifier : String

    /**
      * Returns the partition columns of the Delta table. In classic mode these are taken from the Delta log snapshot,
      * in Connect mode - where DeltaLog is not reachable - from the `DESCRIBE DETAIL` SQL command which runs on the
      * server.
      */
    protected def deltaPartitionColumns(execution: Execution) : Seq[String] = {
        if (SparkBackend.of(execution.spark).isConnect)
            execution.spark.sql(s"DESCRIBE DETAIL $sqlIdentifier").select("partitionColumns").head().getSeq[String](0)
        else
            DeltaShim.snapshot(deltaCatalogTable(execution)).metadata.partitionColumns
    }

    protected def replaceWhere(df: DataFrame, partitionSpec: PartitionSpec, mode: OutputMode) : String = {
        if (partitionSpec.nonEmpty) {
            partitionSpec.predicate
        }
        else if (partitions.nonEmpty && mode == OutputMode.OVERWRITE_DYNAMIC) {
            val pcols = partitions.map(_.name)
            val parts = df.select(pcols.map(c => df(c)): _*)
                .distinct()
                .collect()
                .map(_.toSeq.map(HiveDialect.literal).mkString("(", ",", ")"))
            pcols.mkString("(", ",", ")") + s" IN (${parts.mkString(",")})"
        }
        else {
            ""
        }
    }

    /**
     * Performs a merge operation. Either you need to specify a [[mergeKey]], or the relation needs to provide some
     * default key.
     *
     * @param execution
     * @param df
     * @param mergeCondition
     * @param clauses
     */
    override def merge(execution: Execution, df: DataFrame, condition:Option[Column], clauses: Seq[MergeClause]): Unit = {
        if (SparkBackend.of(execution.spark).isConnect)
            mergeConnect(execution, df, condition, clauses)
        else {
            val mergeCondition = condition.getOrElse {
                val withinPartitionKeyColumns =
                    if (mergeKey.nonEmpty)
                        mergeKey
                    else if (schema.exists(_.primaryKey.nonEmpty))
                        schema.map(_.primaryKey).get
                    else
                        throw new IllegalArgumentException(s"Merging Delta relation '$identifier' requires primary key in schema, explicit merge key or merge condition")
                (SetIgnoreCase(partitions.map(_.name)) ++ withinPartitionKeyColumns)
                    .toSeq
                    .map(k => col("target." + k) <=> col("source." + k))
                    .reduce(_ && _)
            }
            val table = deltaTable(execution)
            DeltaUtils.merge(table, df, mergeCondition, clauses)
        }
    }

    /**
      * Runs a merge against the (Connect) server as a MERGE SQL statement. The source DataFrame is registered as a
      * temporary view, because a Connect client cannot build the DeltaTable-.merge() plan locally (io.delta.tables
      * reaches into the driver session state).
      */
    private def mergeConnect(execution: Execution, df: DataFrame, condition: Option[Column], clauses: Seq[MergeClause]): Unit = {
        val sourceView = "_flowman_delta_merge_source"
        DataFrameUtils.withTempView(sourceView, df) {
            val conditionSql = condition.map(_.toString).getOrElse {
                val withinPartitionKeyColumns =
                    if (mergeKey.nonEmpty)
                        mergeKey
                    else if (schema.exists(_.primaryKey.nonEmpty))
                        schema.map(_.primaryKey).get
                    else
                        throw new IllegalArgumentException(s"Merging Delta relation '$identifier' requires primary key in schema, explicit merge key or merge condition")
                (SetIgnoreCase(partitions.map(_.name)) ++ withinPartitionKeyColumns)
                    .toSeq
                    .map(k => s"target.${DeltaUtils.quoteIdentifier(k)} IS NOT DISTINCT FROM source.${DeltaUtils.quoteIdentifier(k)}")
                    .mkString(" AND ")
            }
            val clauseSqls = clauses.map {
                case InsertClause(c, columns) =>
                    val when = c.map(cc => s" AND ${cc.toString}").getOrElse("")
                    if (columns.nonEmpty) {
                        val names = columns.keys.toSeq.map(DeltaUtils.quoteIdentifier).mkString(", ")
                        val values = columns.values.toSeq.map(_.toString).mkString(", ")
                        s" WHEN NOT MATCHED$when THEN INSERT ($names) VALUES ($values)"
                    }
                    else {
                        s" WHEN NOT MATCHED$when THEN INSERT *"
                    }
                case UpdateClause(c, columns) =>
                    val when = c.map(cc => s" AND ${cc.toString}").getOrElse("")
                    if (columns.nonEmpty) {
                        val sets = columns.map { case (k, v) => s"${DeltaUtils.quoteIdentifier(k)} = ${v.toString}" }.mkString(", ")
                        s" WHEN MATCHED$when THEN UPDATE SET $sets"
                    }
                    else {
                        s" WHEN MATCHED$when THEN UPDATE SET *"
                    }
                case DeleteClause(c) =>
                    val when = c.map(cc => s" AND ${cc.toString}").getOrElse("")
                    s" WHEN MATCHED$when THEN DELETE"
            }
            DeltaUtils.runMerge(execution.spark, sqlIdentifier, sourceView, conditionSql, clauseSqls)
        }
    }

    /**
     * Reads data from a streaming source
     *
     * @param execution
     * @param schema
     * @return
     */
    protected def readStreamFrom(execution: Execution, location: Path): DataFrame = {
        streamReader(execution, "delta", options).load(location.toString)
    }

    /**
     * Writes data to a streaming sink
     *
     * @param execution
     * @param df
     * @return
     */
    protected def writeStreamTo(execution: Execution, df: DataFrame, location: Path, mode: OutputMode, trigger: Trigger, checkpointLocation: Path): StreamingQuery = {
        val writer = streamWriter(execution, df, "delta", options, mode.streamMode, trigger, checkpointLocation)
        if (partitions.nonEmpty) {
            writer
                .partitionBy(partitions.map(_.name):_*)
                .start(location.toString)
        }
        else {
            writer.start(location.toString)
        }
    }

    /**
     * Performs actual migration
     * @param execution
     * @param migrationPolicy
     * @param migrationStrategy
     */
    protected def migrateInternal(execution: Execution): Unit = {
        val sourceSchema = com.dimajix.flowman.types.StructType.of(deltaSchema(execution))
        val targetSchema = fullSchema.get
        val sourceTable = TableDefinition(
            TableIdentifier.empty,
            columns = sourceSchema.fields,
            partitionColumnNames = deltaPartitionColumns(execution)
        )
        val targetTable = TableDefinition(
            TableIdentifier.empty,
            columns = targetSchema.fields,
            partitionColumnNames = partitions.map(_.name)
        )

        val requiresMigration = TableChange.requiresMigration(sourceTable, targetTable, migrationPolicy)

        if (requiresMigration) {
            doMigration(execution, sourceTable, targetTable)
            execution.refreshResource(resource)
        }
    }
    private def doMigration(execution: Execution, currentTable:TableDefinition, targetTable:TableDefinition) : Unit = {
        migrationStrategy match {
            case MigrationStrategy.NEVER =>
                logger.warn(s"Migration required for Delta relation '$identifier', but migrations are disabled.\nCurrent schema:\n${currentTable.schema.treeString}New schema:\n${targetTable.schema.treeString}")
            case MigrationStrategy.FAIL =>
                logger.error(s"Cannot migrate Delta relation '$identifier', since migrations are disabled.\nCurrent schema:\n${currentTable.schema.treeString}New schema:\n${targetTable.schema.treeString}")
                throw new MigrationFailedException(identifier)
            case MigrationStrategy.ALTER =>
                val migrations = TableChange.migrate(currentTable, targetTable, migrationPolicy)
                if (migrations.exists(m => !supported(m))) {
                    val unsupportedChanges = migrations.filter(m => !supported(m))
                    logger.error(s"Cannot migrate Delta relation '$identifier', since that would require unsupported changes.\n"+
                        s"Current schema:\n${currentTable.schema.treeString}" +
                        s"New schema:\n${targetTable.schema.treeString}" +
                        s"Unsupported changes:\n${unsupportedChanges.map(m => " - " + m.toString).mkString("\n")}")
                    throw new MigrationFailedException(identifier)
                }
                alter(migrations)
            case MigrationStrategy.ALTER_REPLACE =>
                val migrations = TableChange.migrate(currentTable, targetTable, migrationPolicy)
                if (migrations.forall(m => supported(m))) {
                    alter(migrations)
                }
                else {
                    recreate()
                }
            case MigrationStrategy.REPLACE =>
                recreate()
        }

        def alter(migrations:Seq[TableChange]) : Unit = {
            logger.info(s"Migrating Delta relation '$identifier'. New schema:\n${targetTable.schema.treeString}")

            try {
                if (SparkBackend.of(execution.spark).isConnect)
                    alterConnect(migrations)
                else
                    alterClassic(migrations)
            }
            catch {
                case NonFatal(ex) => throw new MigrationFailedException(identifier, ex)
            }
        }

        /**
          * Migrates the Delta table via SQL DDL which runs on the (Connect) server. Classic-only Catalyst commands
          * (AlterTableAddColumnsDeltaCommand / AlterTableChangeColumnDeltaCommand) are not reachable from a Connect
          * client, so the same changes are expressed as ALTER TABLE statements.
          */
        def alterConnect(migrations:Seq[TableChange]) : Unit = {
            val spark = execution.spark
            migrations.foreach {
                case AddColumn(column) =>
                    val colDef = DeltaUtils.colDef(column.catalogField)
                    spark.sql(s"ALTER TABLE $sqlIdentifier ADD COLUMNS ($colDef)")
                case UpdateColumnNullability(column, nullable) =>
                    val q = DeltaUtils.quoteIdentifier(column)
                    val action = if (nullable) "DROP NOT NULL" else "SET NOT NULL"
                    spark.sql(s"ALTER TABLE $sqlIdentifier ALTER COLUMN $q $action")
                case UpdateColumnType(column, dataType, _, _) =>
                    val q = DeltaUtils.quoteIdentifier(column)
                    spark.sql(s"ALTER TABLE $sqlIdentifier CHANGE COLUMN $q $q ${dataType.catalogType.sql}")
                case UpdateColumnComment(column, comment) =>
                    val q = DeltaUtils.quoteIdentifier(column)
                    val currentType = deltaSchema(execution)(column).dataType.sql
                    val commentClause = comment.map(c => s" COMMENT '${DeltaUtils.quoteString(c)}'").getOrElse("")
                    spark.sql(s"ALTER TABLE $sqlIdentifier CHANGE COLUMN $q $q $currentType$commentClause")
                case m => throw new UnsupportedOperationException(s"Migration $m not supported by Delta relations")
            }
        }

        def alterClassic(migrations:Seq[TableChange]) : Unit = {
            val spark = execution.spark
            migrations.foreach {
                case AddColumn(column) =>
                    val table = deltaCatalogTable(execution)
                    AlterTableAddColumnsDeltaCommand(
                        table,
                        Seq(QualifiedColumn(column.name, column.catalogType, column.nullable, column.description)
                        )).run(spark)
                case UpdateColumnNullability(column, nullable) =>
                    val table = deltaCatalogTable(execution)
                    val oldColumn = table.schema()(column)
                    AlterTableChangeColumnDeltaCommand(
                        table,
                        Seq(),
                        column,
                        oldColumn.copy(nullable=nullable),
                        None
                    ).run(spark)
                case UpdateColumnType(column, dataType, _, _) =>
                    val table = deltaCatalogTable(execution)
                    val oldColumn = table.schema()(column)
                    AlterTableChangeColumnDeltaCommand(
                        table,
                        Seq(),
                        column,
                        oldColumn.copy(dataType=dataType.catalogType),
                        None
                    ).run(spark)
                case UpdateColumnComment(column, comment) =>
                    val table = deltaCatalogTable(execution)
                    val oldColumn = table.schema()(column)
                    val field = comment.map(c => oldColumn.withComment(c))
                        .getOrElse(oldColumn.copy(metadata = new MetadataBuilder().withMetadata(oldColumn.metadata).remove("comment").build()))
                    AlterTableChangeColumnDeltaCommand(
                        table,
                        Seq(),
                        column,
                        field,
                        None
                    ).run(spark)
                case m => throw new UnsupportedOperationException(s"Migration $m not supported by Delta relations")
            }
        }

        def recreate() : Unit = {
            logger.info(s"Migrating Delta relation '$identifier' by dropping/recreating.")
            try {
                destroy(execution)
                create(execution)
            }
            catch {
                case NonFatal(ex) => throw new MigrationFailedException(identifier, ex)
            }
        }

        def supported(change:TableChange) : Boolean = {
            change match {
                case _:DropColumn => false
                case a:AddColumn => a.column.nullable // Only allow nullable columns to be added
                case _:UpdateColumnNullability => true
                case _:UpdateColumnType => false
                case _:UpdateColumnComment => true
                case _:PartitionChange => false
                case x:TableChange => throw new UnsupportedOperationException(s"Table change ${x} not supported")
            }
        }
    }

    override protected def outputSchema(execution:Execution) : Option[StructType] = {
        Some(deltaSchema(execution))
    }
}
