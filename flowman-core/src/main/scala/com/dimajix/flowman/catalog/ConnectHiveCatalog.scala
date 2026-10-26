/*
 * Copyright (C) 2018 The Flowman Authors
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

package com.dimajix.flowman.catalog

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.DatabaseAlreadyExistsException
import org.apache.spark.sql.catalyst.analysis.NoSuchDatabaseException
import org.apache.spark.sql.catalyst.analysis.NoSuchPartitionException
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException
import org.slf4j.LoggerFactory

import com.dimajix.common.MapIgnoreCase
import com.dimajix.common.SetIgnoreCase
import com.dimajix.flowman.config.Configuration
import com.dimajix.flowman.fs.HadoopUtils
import com.dimajix.flowman.model.PartitionField
import com.dimajix.flowman.model.PartitionSchema
import com.dimajix.flowman.types.DateType
import com.dimajix.flowman.types.FieldType
import com.dimajix.flowman.types.Field
import com.dimajix.flowman.types.FieldValue
import com.dimajix.flowman.types.TimestampType


/**
  * A [[Catalog]] implementation which works against a Spark Connect client. It cannot access the server-side
  * session state, but can use SQL statements and the Dataset API instead. Consequently it is not as powerful
  * as the [[ClassicHiveCatalog]], but it supports the most important DDL operations required by Flowman.
  *
  * @param spark
  * @param config
  */
final class ConnectHiveCatalog(val spark:SparkSession, val config:Configuration) extends Catalog {
    private val logger = LoggerFactory.getLogger(classOf[ConnectHiveCatalog])

    private val hadoopConf = {
        val sparkConf = new org.apache.spark.SparkConf(false)
        spark.conf.getAll.foreach { case (key, value) => sparkConf.set(key, value) }
        org.apache.spark.sql.SparkShim.getHadoopConf(sparkConf)
    }

    private case class DescribeRow(col: String, dataType: String, comment: String)

    private case class TableDescription(
        tableType: TableType,
        external: Boolean,
        columns: Seq[Field],
        partitionColumnNames: Seq[String],
        comment: Option[String],
        provider: Option[String],
        location: Option[Path],
        viewText: Option[String],
        inputFormat: Option[String],
        outputFormat: Option[String],
        serde: Option[String],
        serdeProperties: Map[String, String],
        tableProperties: Map[String, String]
    )


    override def currentDatabase : String = {
        spark.sql("SELECT current_database()").collect().head.getString(0)
    }

    override def listDatabases() : Seq[String] = {
        spark.sql("SHOW DATABASES").collect().map(_.getString(0))
    }

    override def createDatabase(database:String, catalog:String, location:Option[Path], ignoreIfExists:Boolean) : Unit = {
        require(database != null && database.nonEmpty)

        val exists = databaseExists(database)
        if (!ignoreIfExists && exists) {
            throw new DatabaseAlreadyExistsException(database)
        }

        if (!exists) {
            logger.info(s"Creating Hive database $database")
            val ifNotExists = if (ignoreIfExists) "IF NOT EXISTS " else ""
            val locationClause = location.map(l => s" LOCATION '${quoteString(l.toString)}'").getOrElse("")
            spark.sql(s"CREATE DATABASE $ifNotExists${quoteIdentifier(database)}$locationClause")
        }
    }

    override def databaseExists(database:String) : Boolean = {
        require(database != null)
        listDatabases().exists(_.equalsIgnoreCase(database))
    }

    override def dropDatabase(database:String, ignoreIfNotExists:Boolean) : Unit = {
        require(database != null && database.nonEmpty)

        val exists = databaseExists(database)
        if (ignoreIfNotExists && !exists) {
            throw new NoSuchDatabaseException(database)
        }

        if (exists) {
            logger.info(s"Dropping Hive database $database")
            val ifExists = if (ignoreIfNotExists) "IF EXISTS " else ""
            spark.sql(s"DROP DATABASE $ifExists${quoteIdentifier(database)} CASCADE")
        }
    }

    override def listTables(database:String) : Seq[TableIdentifier] = {
        val db = quoteIdentifier(database)
        spark.sql(s"SHOW TABLES IN $db")
            .collect()
            .map { row =>
                if (row.length >= 3 && !row.getBoolean(2))
                    row.getString(1)
                else if (row.length >= 2)
                    row.getString(1)
                else
                    row.getString(0)
            }
            .map(name => TableIdentifier(name, Seq(database)))
    }

    override def listTables(database:String, pattern:String) : Seq[TableIdentifier] = {
        listTables(database).filter(t => wildcardMatches(pattern, t.table))
    }

    override def createTable(table:TableDefinition, ignoreIfExists:Boolean) : Unit = {
        require(table != null)

        val exists = tableExists(table.identifier)
        if (!ignoreIfExists && exists) {
            throw new TableAlreadyExistsException(table.identifier.database.getOrElse(""), table.identifier.table)
        }

        if (!exists) {
            logger.info(s"Creating Hive table ${table.identifier}")

            val ifNotExists = if (ignoreIfExists) "IF NOT EXISTS " else ""
            val external = if (table.external) "EXTERNAL " else ""

            val colDefs = table.dataColumns.map(colDef).mkString(",\n    ")
            val partDefs = table.partitionColumns.map(colDef).mkString(",\n    ")
            val partitionedBy = if (partDefs.nonEmpty) s"\nPARTITIONED BY (\n    $partDefs\n)" else ""

            val comment = table.comment.map(c => s"\nCOMMENT '${quoteString(c)}'").getOrElse("")

            val rowFormat = {
                val serde = table.serde.map(s => s"ROW FORMAT SERDE '${quoteString(s)}'")
                val serdeProps = table.serdeProperties.toSeq.sortBy(_._1)
                    .map { case (k, v) => s"'${quoteString(k)}' = '${quoteString(v)}'" }
                    .mkString(", ")
                val serdePropertiesClause = if (serdeProps.nonEmpty) s" WITH SERDEPROPERTIES ($serdeProps)" else ""
                (table.inputFormat, table.outputFormat) match {
                    case (Some(_), Some(_)) =>
                        Some(s"${serde.getOrElse("ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'")}$serdePropertiesClause\nSTORED AS INPUTFORMAT '${quoteString(table.inputFormat.get)}' OUTPUTFORMAT '${quoteString(table.outputFormat.get)}'")
                    case _ =>
                        serde.map(s => s"$s$serdePropertiesClause")
                }
            }
            val storedAs = if (rowFormat.isEmpty) {
                if (table.provider.contains("parquet"))
                    Some("\nSTORED AS PARQUET")
                else if (table.provider.contains("hive"))
                    Some("\nSTORED AS TEXTFILE")
                else
                    None
            }
            else {
                None
            }

            val location = table.location.map(l => s"\nLOCATION '${quoteString(l.toString)}'").getOrElse("")

            val tblProperties = table.tableProperties.toSeq.sortBy(_._1)
                .map { case (k, v) => s"'${quoteString(k)}' = '${quoteString(v)}'" }
                .mkString(", ")
            val tablePropertiesClause = if (tblProperties.nonEmpty) s"\nTBLPROPERTIES ($tblProperties)" else ""

                val sql = s"CREATE $external TABLE $ifNotExists${table.identifier.quotedString} (\n    $colDefs\n)" +
                partitionedBy +
                comment +
                rowFormat.map(s => s"\n$s").getOrElse("") +
                storedAs.getOrElse("") +
                location +
                tablePropertiesClause
            spark.sql(sql)
        }
    }

    override def refreshTable(table:TableIdentifier) : Unit = {
        require(table != null)
        logger.info(s"Refreshing Hive table $table")
        spark.sql(s"REFRESH TABLE ${table.quotedString}")
    }

    override def tableExists(name:TableIdentifier) : Boolean = {
        val db = name.database.getOrElse(currentDatabase)
        val like = name.table.replace("\\", "\\\\").replace("_", "\\_").replace("%", "\\%")
        val rows = spark.sql(s"SHOW TABLES IN ${quoteIdentifier(db)} LIKE '${quoteString(like)}'").collect()
        rows.exists(row => {
            val tableName = if (row.length >= 2) row.getString(1) else row.getString(0)
            tableName.equalsIgnoreCase(name.table)
        })
    }

    override def getTable(name:TableIdentifier) : TableDefinition = {
        val description = describeTable(name)
        val id = TableIdentifier(name.table, Seq(name.database.getOrElse(currentDatabase)))
        TableDefinition(
            identifier = id,
            tableType = description.tableType,
            columns = description.columns,
            partitionColumnNames = description.partitionColumnNames,
            comment = description.comment,
            provider = description.provider,
            location = description.location,
            external = description.external,
            tableProperties = description.tableProperties,
            viewText = description.viewText,
            inputFormat = description.inputFormat,
            outputFormat = description.outputFormat,
            serde = description.serde,
            serdeProperties = description.serdeProperties
        )
    }

    override def getTableLocation(name:TableIdentifier) : Path = {
        val table = getTable(name)
        require(table.tableType != TableType.VIEW)
        table.location.getOrElse(throw new NoSuchTableException(name.database.getOrElse(""), name.table))
    }

    override def dropTable(table:TableIdentifier, ignoreIfNotExists:Boolean=false) : Unit = {
        require(table != null)
        logger.info(s"Dropping Hive table $table")
        if (tableExists(table)) {
            val tableDefinition = getTable(table)
            if (tableDefinition.tableType == TableType.VIEW)
                throw new NoSuchTableException(table.database.getOrElse(""), table.table)

            // Delete all partitions
            if (tableDefinition.partitionColumns.nonEmpty) {
                listPartitions(table).foreach { partition =>
                    val location = getPartitionLocation(table, partition)
                    val fs = location.getFileSystem(hadoopConf)
                    HadoopUtils.deleteLocation(fs, location)
                    dropPartition(table, partition)
                }
            }

            // Drop the table itself
            val ifExists = if (ignoreIfNotExists) "IF EXISTS " else ""
            spark.sql(s"DROP TABLE $ifExists${table.quotedString}")

            // Delete the location to clean up any remaining files (Spark will not delete the data
            // of an EXTERNAL table on DROP)
            tableDefinition.location.foreach { location =>
                val fs = location.getFileSystem(hadoopConf)
                HadoopUtils.deleteLocation(fs, location)
            }
        }
        else if (!ignoreIfNotExists) {
            throw new NoSuchTableException(table.database.getOrElse(""), table.table)
        }
    }

    override def truncateTable(table:TableIdentifier) : Unit = {
        require(table != null)
        logger.info(s"Truncating Hive table $table")

        val tableDefinition = getTable(table)
        require(tableDefinition.tableType != TableType.VIEW)

        // First drop partitions (Hive will delete the partition directories as well)
        if (tableDefinition.partitionColumns.nonEmpty) {
            listPartitions(table).foreach(p => dropPartition(table, p, false))
        }

        // Then clean up the table location from any remainders
        val location = tableDefinition.location.getOrElse(throw new NoSuchTableException(table.database.getOrElse(""), table.table))
        val fs = location.getFileSystem(hadoopConf)
        HadoopUtils.truncateLocation(fs, location)

        refreshTable(table)
    }

    override def alterTable(table:TableIdentifier, changes:Seq[TableChange]) : Unit = {
        require(table != null)
        require(changes != null)

        val catalogTable = getTable(table)
        require(catalogTable.tableType != TableType.VIEW)

        val colsToAdd = changes.collect { case a:TableChange.AddColumn => a.column }
        if (colsToAdd.nonEmpty) {
            addTableColumns(table, colsToAdd)
        }

        changes.foreach {
            case _:TableChange.AddColumn =>
            case u:TableChange.UpdateColumnNullability =>
                updateColumnNullability(table, u.column, u.nullable)
            case u:TableChange.UpdateColumnComment =>
                updateColumnComment(table, u.column, u.comment.getOrElse(""))
            case x:TableChange => throw new UnsupportedOperationException(s"Unsupported table change $x for Hive table $table")
        }
    }

    override def addTableColumns(table:TableIdentifier, colsToAdd:Seq[Field]) : Unit = {
        require(table != null)
        require(colsToAdd != null)
        logger.info(s"Adding new columns ${colsToAdd.map(_.name).mkString(",")} to existing Hive table '$table'")
        val colDefs = colsToAdd.map(colDef).mkString(",\n    ")
        spark.sql(s"ALTER TABLE ${table.quotedString} ADD COLUMNS (\n    $colDefs\n)")
    }

    override def partitionExists(table:TableIdentifier, partition:PartitionSpec) : Boolean = {
        require(table != null)
        require(partition != null)
        val parts = listPartitions(table)
        val fields = getTable(table).partitionSchema.fields.map(f => f.name -> f.ftype).toMap
        parts.exists { part =>
            partition.values.forall { case (key, value) =>
                part.values.get(key).exists { partValue =>
                    normalizePartitionValue(fields.get(key), partValue.toString)
                        .equalsIgnoreCase(normalizePartitionValue(fields.get(key), FieldValue.asString(value)))
                }
            }
        }
    }

    private def normalizePartitionValue(fieldType:Option[FieldType], value:String) : String = {
        fieldType match {
            case Some(TimestampType) =>
                val text = value.toUpperCase.replace("Z", "").replace("T", " ")
                val dt = if (text.length > 16) text.substring(0, 19) else text + (":00" * (3 - text.count(_ == ':')))
                dt
            case Some(DateType) if value.nonEmpty =>
                value.take(10)
            case _ => value
        }
    }

    override def getPartition(table:TableIdentifier, partition:PartitionSpec) : PartitionDefinition = {
        require(table != null)
        require(partition != null && partition.nonEmpty)
        val description = describeTable(table, Some(partition))
        description.location
            .map(location => PartitionDefinition(location))
            .getOrElse(throw new NoSuchPartitionException(table.database.getOrElse(""), table.table, partition.catalogPartition))
    }

    override def getPartitionLocation(table:TableIdentifier, partition:PartitionSpec) : Path = {
        getPartition(table, partition).location
    }

    override def getPartitionSchema(table:TableIdentifier) : PartitionSchema = {
        require(table != null)
        val definition = getTable(table)
        val partitionNames = SetIgnoreCase(definition.partitionColumnNames)
        val fields = definition.columns.filter(c => partitionNames.contains(c.name))
        PartitionSchema(fields.map(f => PartitionField.fromSpark(f.catalogField)))
    }

    override def addPartition(table:TableIdentifier, partition:PartitionSpec, location:Path) : Unit = {
        require(table != null)
        require(partition != null && partition.nonEmpty)
        require(location != null && location.toString.nonEmpty)
        if (partitionExists(table, partition)) {
            throw new TableAlreadyExistsException(table.database.getOrElse(""), table.table)
        }
        logger.info(s"Adding partition ${partition.spec} of table $table with location $location")
        spark.sql(s"ALTER TABLE ${table.quotedString} ADD PARTITION (${partitionSpec(partition)}) LOCATION '${quoteString(location.toString)}'")
    }

    override def addOrReplacePartition(table:TableIdentifier, partition:PartitionSpec, location:Path) : Unit = {
        require(table != null)
        require(partition != null && partition.nonEmpty)
        require(location != null && location.toString.nonEmpty)
        if (partitionExists(table, partition))
            updatePartitionLocation(table, partition, location)
        else
            addPartition(table, partition, location)
    }

    override def refreshPartition(table:TableIdentifier, partition:PartitionSpec) : Unit = {
        require(table != null)
        require(partition != null && partition.nonEmpty)
        logger.info(s"Refreshing partition ${partition.spec} of table $table")
        spark.sql(s"REFRESH TABLE ${table.quotedString}")
    }

    override def truncatePartition(table:TableIdentifier, partition:PartitionSpec) : Unit = {
        require(table != null)
        require(partition != null && partition.nonEmpty)
        logger.info(s"Truncating partition ${partition.spec} of table $table")
        // The Connect client cannot directly manipulate the file system, so we remove the partition and
        // recreate it with the same location
        val location = getPartitionLocation(table, partition)
        dropPartition(table, partition, true)
        addPartition(table, partition, location)
    }

    override def dropPartition(table:TableIdentifier, partition:PartitionSpec, ignoreIfNotExists:Boolean=false) : Unit = {
        dropPartitions(table, Seq(partition), ignoreIfNotExists)
    }

    override def dropPartitions(table:TableIdentifier, partitions:Seq[PartitionSpec], ignoreIfNotExists:Boolean=false) : Unit = {
        require(table != null)
        require(partitions != null)
        val ifExists = if (ignoreIfNotExists) "IF EXISTS " else ""
        partitions.foreach { partition =>
            spark.sql(s"ALTER TABLE ${table.quotedString} DROP $ifExists PARTITION (${partitionSpec(partition)})")
        }
    }

    private def listPartitions(table:TableIdentifier) : Seq[PartitionSpec] = {
        require(table != null)
        val rows = spark.sql(s"SHOW PARTITIONS ${table.quotedString}").collect().map(_.getString(0))
        rows.map { row =>
            val values = row.split("/").map { kv =>
                val idx = kv.indexOf('=')
                val key = if (idx >= 0) kv.substring(0, idx) else kv
                val value = if (idx >= 0) java.net.URLDecoder.decode(kv.substring(idx + 1), "UTF-8") else ""
                key -> value
            }.toMap
            PartitionSpec(MapIgnoreCase(values))
        }
    }

    override def createView(table:TableIdentifier, select:String, ignoreIfExists:Boolean): Unit = {
        require(table != null)
        if (tableExists(table)) {
            throw new TableAlreadyExistsException(table.database.getOrElse(""), table.table)
        }
        val orReplace = if (ignoreIfExists) "OR REPLACE " else ""
        spark.sql(s"CREATE $orReplace VIEW ${table.quotedString} AS $select")
    }

    override def alterView(table:TableIdentifier, select:String): Unit = {
        require(table != null)
        spark.sql(s"ALTER VIEW ${table.quotedString} AS $select")
    }

    override def dropView(table:TableIdentifier, ignoreIfNotExists:Boolean=false) : Unit = {
        require(table != null)
        if (ignoreIfNotExists && !tableExists(table)) {
            throw new NoSuchTableException(table.database.getOrElse(""), table.table)
        }
        spark.sql(s"DROP VIEW IF EXISTS ${table.quotedString}")
    }


    private def updatePartitionLocation(table:TableIdentifier, partition:PartitionSpec, location:Path) : Unit = {
        spark.sql(s"ALTER TABLE ${table.quotedString} PARTITION (${partitionSpec(partition)}) SET LOCATION '${quoteString(location.toString)}'")
    }

    private def updateColumnNullability(table:TableIdentifier, column:String, nullable:Boolean) : Unit = {
        val field = columnField(table, column)
        val nullability = if (nullable) "DROP NOT NULL" else "SET NOT NULL"
        spark.sql(s"ALTER TABLE ${table.quotedString} ALTER COLUMN ${quoteIdentifier(column)} TYPE ${field.catalogType.sql} $nullability")
    }

    private def updateColumnComment(table:TableIdentifier, column:String, comment:String) : Unit = {
        val field = columnField(table, column)
        spark.sql(s"ALTER TABLE ${table.quotedString} ALTER COLUMN ${quoteIdentifier(column)} TYPE ${field.catalogType.sql} COMMENT '${quoteString(comment)}'")
    }

    private def columnField(table:TableIdentifier, column:String) : Field = {
        getTable(table).columns
            .find(_.name.equalsIgnoreCase(column))
            .getOrElse(throw new IllegalArgumentException(s"Table column $column does not exist in table $table"))
    }

    private def colDef(field:Field) : String = {
        val comment = field.description.map(c => s" COMMENT '${quoteString(c)}'").getOrElse("")
        s"${quoteIdentifier(field.name)} ${field.catalogType.sql}$comment"
    }

    private def partitionSpec(partition:PartitionSpec) : String = {
        partition.values.map { case (name, value) =>
            s"${quoteIdentifier(name)} = ${partitionValue(value)}"
        }.mkString(", ")
    }

    private def partitionValue(value:Any) : String = {
        value match {
            case null => "NULL"
            case s:String => s"'${quoteString(s)}'"
            case other =>
                val str = other.toString
                if (str.isEmpty) s"''" else str
        }
    }

    private def describeTable(name:TableIdentifier, partition:Option[PartitionSpec] = None) : TableDescription = {
        val partitionClause = partition.map(p => s" PARTITION (${partitionSpec(p)})").getOrElse("")
        val rows = spark.sql(s"DESCRIBE TABLE EXTENDED ${name.quotedString}$partitionClause").collect().toSeq.map { row =>
            DescribeRow(stringValue(row, 0), stringValue(row, 1), stringValue(row, 2))
        }

        val partitionIdx = rows.indexWhere(_.col == "# Partition Information")
        val detailIdx = rows.indexWhere(_.col == "# Detailed Table Information")
        val dataStart = rows.indexWhere(r => !r.col.startsWith("#"))
        val columnsEnd = Seq(partitionIdx, detailIdx).filter(_ >= 0) match {
            case Seq() => rows.length
            case idx => idx.min
        }

        val allColumns = spark.table(name.quotedString).schema.fields.map(Field.of)

        val partitionRows = if (partitionIdx >= 0 && detailIdx > partitionIdx) {
            rows.slice(partitionIdx + 1, detailIdx)
                .filter(r => r.col.nonEmpty && !r.col.startsWith("#") && r.col != "col_name")
        }
        else {
            Seq.empty
        }
        val partitionNames = partitionRows.map(_.col)
        val partitionNameSet = SetIgnoreCase(partitionNames)

        // The partition columns of a Hive table are always NOT NULL. The `spark.table(...)` schema however
        // reports the analyzed (=nullable) schema, so recover the NOT NULL contract from the catalog for the
        // partition columns just as Spark's own catalog schema does.
        val allColumnsFixed = allColumns.map { field =>
            if (partitionNameSet.contains(field.name))
                field.copy(nullable = false)
            else
                field
        }

        val detail = rows.slice(detailIdx + 1, rows.length)
            .filter(r => r.col.nonEmpty)
            .map(r => r.col -> r.dataType)
            .toMap

        val tableType = detail.get("Type") match {
            case Some("VIEW") => TableType.VIEW
            case Some(_) => TableType.TABLE
            case None => TableType.UNKNOWN
        }

        TableDescription(
            tableType = tableType,
            external = detail.get("Type").contains("EXTERNAL"),
            columns = allColumnsFixed,
            partitionColumnNames = partitionNames,
            comment = detail.get("Comment").filter(_.nonEmpty),
            provider = detail.get("Provider"),
            location = detail.get("Location").filter(_.nonEmpty).map(loc => {
                    new Path(loc)
            }),
            viewText = detail.get("View Text").filter(_.nonEmpty),
            inputFormat = detail.get("InputFormat").filter(_.nonEmpty),
            outputFormat = detail.get("OutputFormat").filter(_.nonEmpty),
            serde = detail.get("Serde Library").filter(_.nonEmpty),
            serdeProperties = parseProperties(detail.get("Storage Properties")),
            tableProperties = parseProperties(detail.get("Table Properties"))
        )
    }

    private def parseProperties(value:Option[String]) : Map[String,String] = {
        value.toSeq.flatMap { str =>
            val content = str.stripPrefix("[").stripSuffix("]")
            if (content.trim.isEmpty)
                Seq.empty
            else
                content.split(",").map { element =>
                    val idx = element.indexOf('=')
                    if (idx < 0)
                        (element.replaceAll("^\\s+", ""), "")
                    else
                        (element.substring(0, idx).replaceAll("^\\s+", ""), element.substring(idx + 1))
                }
        }.toMap
    }

    private def stringValue(row:org.apache.spark.sql.Row, index:Int) : String = {
        if (row.isNullAt(index)) "" else row.getString(index)
    }

    private def wildcardMatches(pattern:String, value:String) : Boolean = {
        value.matches("^" + pattern.replace("\\", "\\\\").replace(".", "\\.").replace("_", ".").replace("%", ".*") + "$")
    }

    private def quoteIdentifier(name:String) : String = {
        s"`${name.replace("`", "``")}`"
    }

    private def quoteString(value:String) : String = {
        value.replace("\\", "\\\\").replace("'", "\\'")
    }
}
