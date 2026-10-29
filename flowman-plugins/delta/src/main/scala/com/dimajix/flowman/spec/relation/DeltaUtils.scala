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

import io.delta.tables.DeltaTable
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkShim
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogStorageFormat
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.catalog.CatalogTableType
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.DeltaOperations
import org.apache.spark.sql.delta.actions.Metadata
import org.apache.spark.sql.delta.commands.CreateDeltaTableCommand
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

import com.dimajix.flowman.catalog.PartitionSpec
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.execution.SparkBackend
import com.dimajix.flowman.execution.MergeClause
import com.dimajix.flowman.execution.DeleteClause
import com.dimajix.flowman.execution.InsertClause
import com.dimajix.flowman.execution.UpdateClause
import com.dimajix.flowman.model.PartitionField
import com.dimajix.flowman.types.FieldValue


object DeltaUtils {
    private[this] val logger = LoggerFactory.getLogger("com.dimajix.flowman.spec.relation.DeltaUtils")

    private[spec] def quoteIdentifier(name:String) : String = s"`${name.replace("`", "``")}`"
    private[spec] def quoteString(value:String) : String = value.replace("\\", "\\\\").replace("'", "\\'")

    private[spec] def colDef(field:StructField) : String = {
        val notNull = if (!field.nullable) " NOT NULL" else ""
        val comment = field.getComment().map(c => s" COMMENT '${quoteString(c)}'").getOrElse("")
        s"${quoteIdentifier(field.name)} ${field.dataType.sql}$notNull$comment"
    }

    /**
      * Returns the schema of the given Delta table via the public io.delta.tables.DeltaTable API (classic driver only;
      * see isDeltaTable for the Connect-mode note).
      */
    private[spec] def deltaTableSchema(deltaTable: DeltaTable) : StructType = {
        deltaTable.toDF.schema
    }

    /**
      * Returns true if the specified location contains a Delta table. In Connect mode the classic
      * `DeltaTable.isDeltaTable` client API is not available (it reaches into the session state on the driver), so the
      * check falls back to looking for the Delta log directory on the (client-visible) file system.
      */
    def isDeltaTable(execution: Execution, location: Path) : Boolean = {
        if (SparkBackend.of(execution.spark).isConnect) {
            val fs = location.getFileSystem(execution.hadoopConf)
            fs.exists(location) && fs.exists(new Path(location, "_delta_log"))
        }
        else {
            DeltaTable.isDeltaTable(execution.spark, location.toString)
        }
    }

    /**
      * Runs a Delta MERGE statement against the Connect server. The source data has to be registered as a temporary
      * view on the client beforehand; the server executes the MERGE as plain SQL against the Delta table.
      */
    def runMerge(
        spark: org.apache.spark.sql.SparkSession,
        targetSql: String,
        sourceView: String,
        condition: String,
        clauses: Seq[String]
    ) : Unit = {
        val builder = new StringBuilder
        builder.append(s"MERGE INTO $targetSql AS target USING $sourceView AS source ON $condition")
        clauses.foreach(builder.append)
        logger.info(s"Executing Delta MERGE statement: ${builder.toString}")
        spark.sql(builder.toString())
    }

    /**
      * Returns the physical location of the given Delta table.
      */
    private def tableIdentifier(location: Path) : TableIdentifier = {
        TableIdentifier(location.toString, Some("delta"))
    }

    def getLocation(execution: Execution, table: com.dimajix.flowman.catalog.TableIdentifier) : Path = {
        execution.catalog.getTableLocation(table)
    }

    /**
      * Returns true if the specified location contains a non-empty Delta table
      * @param execution
      * @param location
      * @return
      */
    def isLoaded(execution: Execution, location: Path) : Boolean = {
        val spark = execution.spark
        if (isDeltaTable(execution, location)) {
            if (SparkBackend.of(spark).isConnect) {
                spark.read.format("delta").load(location.toString).isEmpty == false
            }
            else {
                val deltaLog = DeltaLog.forTable(spark, location)
                val snapshot = deltaLog.snapshot
                DeltaLog.filterFileList(
                    snapshot.metadata.partitionSchema,
                    snapshot.allFiles.toDF(),
                    Seq()).count() > 0
            }
        }
        else {
            false
        }
    }

    /**
      * Returns true if the specified location contains a Delta table with the given partition being non-empty
      * @param execution
      * @param location
      * @return
      */
    def isLoaded(execution: Execution, location: Path, partition: PartitionSpec) : Boolean = {
        val spark = execution.spark
        if (SparkBackend.of(spark).isConnect) {
            val df = spark.read.format("delta").load(location.toString)
            partitionDf(df, partition).isEmpty == false
        }
        else {
            val deltaLog = DeltaLog.forTable(execution.spark, location)
            DeltaUtils.isLoaded(deltaLog, partition)
        }
    }

    /**
      * Returns true if the specified location contains a Delta table with the given partition being non-empty
      * @param execution
      * @param table
      * @return
      */
    def isLoaded(execution: Execution, table: com.dimajix.flowman.catalog.TableIdentifier, partition: PartitionSpec) : Boolean = {
        val location = getLocation(execution, table)
        isLoaded(execution, location, partition)
    }

    /**
      * Returns true if the partition of the given Delta table is non-empty
      * @param execution
      * @param deltaLog
      * @return
      */
    def isLoaded(deltaLog:DeltaLog, partition:PartitionSpec) : Boolean = {
        val partitionFilters = partition
            .values
            .map { case (k, v) => SparkShim.expr(col(k) === lit(v)) }
            .toSeq

        val snapshot = deltaLog.snapshot
        DeltaLog.filterFileList(
            snapshot.metadata.partitionSchema,
            snapshot.allFiles.toDF(),
            partitionFilters).count() > 0
    }

    private def partitionDf(df:DataFrame, partition:PartitionSpec) : DataFrame = {
        if (partition.values.nonEmpty)
            df.where(partition.predicate)
        else
            df
    }

    def upsert(table:DeltaTable, df: DataFrame, keyColumns:Iterable[String], partitionSpec: PartitionSpec) : Unit = {
        if (keyColumns.isEmpty)
            throw new IllegalArgumentException(s"Cannot perform upsert activity without primary key")

        val keyCondition = keyColumns.map(k => col("relation." + k) <=> col("df." + k))
        val partitionCondition = partitionSpec.values.map { case (k, v) =>
            (col("relation." + k) === FieldValue.asLiteral(v))
        }
        val mergeCondition = (keyCondition ++ partitionCondition).reduce(_ && _)
        table.as("relation")
            .merge(df.as("df"), mergeCondition)
            .whenMatched().updateAll()
            .whenNotMatched().insertAll()
            .execute()
    }

    def merge(table:DeltaTable, df: DataFrame, mergeCondition:Column, clauses:Seq[MergeClause]) : Unit = {
        val builder = table.as("target")
            .merge(df.as("source"), mergeCondition)
        clauses.foldLeft(builder) { (mergeBuilder, clause) =>
                clause match {
                    case InsertClause(condition, columns) =>
                        val b2 = condition.map(mergeBuilder.whenNotMatched).getOrElse(mergeBuilder.whenNotMatched())
                        if (columns.nonEmpty)
                            b2.insert(columns)
                        else
                            b2.insertAll()
                    case UpdateClause(condition, columns) =>
                        val b2 = condition.map(mergeBuilder.whenMatched).getOrElse(mergeBuilder.whenMatched())
                        if (columns.nonEmpty)
                            b2.update(columns)
                        else
                            b2.updateAll()
                    case DeleteClause(condition) =>
                        val b2 = condition.map(mergeBuilder.whenMatched).getOrElse(mergeBuilder.whenMatched())
                        b2.delete()
                }

            }
            .execute()
    }

    def createTable(execution: Execution, table:Option[com.dimajix.flowman.catalog.TableIdentifier], location:Option[Path], schema:StructType, partitions: Seq[PartitionField], properties:Map[String,String], description:Option[String]): Unit = {
        if (SparkBackend.of(execution.spark).isConnect)
            createTableConnect(execution, table, location, schema, partitions, properties, description)
        else
            createTableClassic(execution, table, location, schema, partitions, properties, description)
    }

    private def createTableClassic(execution: Execution, table:Option[com.dimajix.flowman.catalog.TableIdentifier], location:Option[Path], schema:StructType, partitions: Seq[PartitionField], properties:Map[String,String], description:Option[String]): Unit = {
        val tableIdentifier = table.map(_.toSpark).getOrElse(this.tableIdentifier(location.get))

        // Configure catalog table by assembling all options
        val tableByPath = table.isEmpty
        val catalogTable = CatalogTable(
            identifier = tableIdentifier,
            tableType = if (location.nonEmpty) CatalogTableType.EXTERNAL else CatalogTableType.MANAGED,
            storage = CatalogStorageFormat(
                locationUri = location.map(_.toUri),
                inputFormat = None,
                outputFormat = None,
                serde = None,
                compressed = false,
                properties = Map()
            ),
            provider = Some("delta"),
            schema = schema,
            partitionColumnNames = partitions.map(_.name),
            properties = properties,
            comment = description
        )

        val cmd = CreateDeltaTableCommand(
            catalogTable,
            None,
            SaveMode.ErrorIfExists,
            None,
            tableByPath = tableByPath
        )
        cmd.run(execution.spark)
    }

    private def createTableConnect(execution: Execution, table:Option[com.dimajix.flowman.catalog.TableIdentifier], location:Option[Path], schema:StructType, partitions: Seq[PartitionField], properties:Map[String,String], description:Option[String]): Unit = {
        val spark = execution.spark
        // By-path tables are referenced as `delta.`<path>`` in Delta SQL; catalog tables via their (qualified) name
        val id = table.map(_.quotedString).getOrElse(s"delta.`${location.get.toString}`")
        val external = if (table.nonEmpty && location.nonEmpty) "EXTERNAL " else ""
        val colDefs = schema.fields.map(colDef).mkString(",\n    ")
        val partitionedBy = if (partitions.nonEmpty)
            s"\nPARTITIONED BY (${partitions.map(p => quoteIdentifier(p.name)).mkString(", ")})"
        else
            ""
        val locationClause = location.filter(_ => table.nonEmpty).map(l => s"\nLOCATION '${quoteString(l.toString)}'").getOrElse("")
        val comment = description.map(c => s"\nCOMMENT '${quoteString(c)}'").getOrElse("")
        val tblProperties = properties.toSeq.sortBy(_._1)
            .map { case (k, v) => s"'${quoteString(k)}' = '${quoteString(v)}'" }
            .mkString(", ")
        val tablePropertiesClause = if (tblProperties.nonEmpty) s"\nTBLPROPERTIES ($tblProperties)" else ""

        spark.sql(
            s"CREATE $external TABLE $id (\n    $colDefs\n) USING delta$partitionedBy$locationClause$comment$tablePropertiesClause"
        )
    }

    def createLog(spark:SparkSession, table:CatalogTable) : Unit = {
        val isManagedTable = table.tableType == CatalogTableType.MANAGED
        val deltaLog = DeltaLog.forTable(spark, new Path(table.location))
        val newMetadata = Metadata(
            description = table.comment.orNull,
            schemaString = table.schema.json,
            partitionColumns = table.partitionColumnNames,
            configuration = table.properties
        )
        val txn = deltaLog.startTransaction()
        txn.updateMetadataForNewTable(newMetadata)
        val op = DeltaOperations.ReplaceTable(newMetadata, isManagedTable, orCreate = false, false)
        txn.commit(Nil, op)
    }
}
