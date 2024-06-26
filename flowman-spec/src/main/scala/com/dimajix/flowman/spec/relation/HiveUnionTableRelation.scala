/*
 * Copyright 2018-2022 Kaya Kupferschmidt
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

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

import com.dimajix.common.MapIgnoreCase
import com.dimajix.common.No
import com.dimajix.common.SetIgnoreCase
import com.dimajix.common.Trilean
import com.dimajix.common.Yes
import com.dimajix.flowman.catalog.TableIdentifier
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.execution.ExecutionException
import com.dimajix.flowman.execution.MigrationFailedException
import com.dimajix.flowman.execution.MigrationPolicy
import com.dimajix.flowman.execution.MigrationStrategy
import com.dimajix.flowman.execution.Operation
import com.dimajix.flowman.execution.OutputMode
import com.dimajix.flowman.execution.UnspecifiedSchemaException
import com.dimajix.flowman.hadoop.FileUtils
import com.dimajix.flowman.jdbc.HiveDialect
import com.dimajix.flowman.model.BaseRelation
import com.dimajix.flowman.model.PartitionField
import com.dimajix.flowman.model.PartitionSchema
import com.dimajix.flowman.model.PartitionedRelation
import com.dimajix.flowman.model.Relation
import com.dimajix.flowman.model.ResourceIdentifier
import com.dimajix.flowman.model.Schema
import com.dimajix.flowman.model.SchemaRelation
import com.dimajix.flowman.transforms.SchemaEnforcer
import com.dimajix.flowman.transforms.CharVarcharPolicy
import com.dimajix.flowman.transforms.UnionTransformer
import com.dimajix.flowman.types.FieldValue
import com.dimajix.flowman.types.SingleValue
import com.dimajix.flowman.util.SchemaUtils
import com.dimajix.spark.sql.catalyst.SqlBuilder


object HiveUnionTableRelation {
    /**
     * Method for generating the UNION SQL. It is moved into the object to be able to unittest it easily
     * @param tables
     * @param schema
     * @return
     */
    private[relation] def unionSql(tables:Seq[DataFrame], schema:StructType) : String = {
        val union = UnionTransformer().transformDataFrames(tables)
        val conformed = SchemaEnforcer(schema,charVarcharPolicy=CharVarcharPolicy.IGNORE).transform(union)
        new SqlBuilder(conformed).toSQL
    }
}

case class HiveUnionTableRelation(
    override val instanceProperties:Relation.Properties,
    override val schema:Option[Schema] = None,
    override val partitions: Seq[PartitionField] = Seq(),
    tablePrefix: TableIdentifier,
    locationPrefix: Option[Path] = None,
    view: TableIdentifier,
    external: Boolean = false,
    format: Option[String] = None,
    options: Map[String,String] = Map(),
    rowFormat: Option[String] = None,
    inputFormat: Option[String] = None,
    outputFormat: Option[String] = None,
    properties: Map[String, String] = Map(),
    serdeProperties: Map[String, String] = Map()
)  extends BaseRelation with SchemaRelation with PartitionedRelation {
    private val logger = LoggerFactory.getLogger(classOf[HiveUnionTableRelation])

    private lazy val tableRegex : TableIdentifier = {
        TableIdentifier(tablePrefix.table + "_[0-9]+", tablePrefix.database.orElse(view.database))
    }
    private lazy val viewIdentifier : TableIdentifier = {
        TableIdentifier(view.table, view.database.orElse(tablePrefix.database))
    }
    private def tableIdentifier(version:Int) : TableIdentifier = {
        TableIdentifier(tablePrefix.table + "_" + version.toString, tablePrefix.database.orElse(view.database))
    }

    private def resolve(execution: Execution, table:TableIdentifier) : TableIdentifier = TableIdentifier(table.table, table.database.orElse(view.database).orElse(Some(execution.catalog.currentDatabase)))

    private def listTables(executor: Execution) : Seq[TableIdentifier] = {
        val catalog = executor.catalog
        val regex = resolve(executor, tableRegex).unquotedString.r
        catalog.listTables(tablePrefix.database.getOrElse(catalog.currentDatabase), tablePrefix.table + "_*")
            .filter { table =>
                table.unquotedString match {
                    case regex() => true
                    case _ => false
                }
            }
    }

    private def tableRelation(version:Int) : HiveTableRelation =
        tableRelation(
            TableIdentifier(tablePrefix.table + "_" + version.toString, tablePrefix.database.orElse(view.database)),
            locationPrefix.map(p => new Path(p.toString + "_" + version.toString))
        )

    private def tableRelation(tableIdentifier: TableIdentifier, location:Option[Path]) : HiveTableRelation = new HiveTableRelation(
        instanceProperties,
        schema,
        partitions,
        tableIdentifier,
        external,
        location,
        format,
        options,
        rowFormat,
        inputFormat,
        outputFormat,
        properties,
        serdeProperties,
        "hive"
    )

    private def viewRelationFromSql(sql:String) : HiveViewRelation = {
        HiveViewRelation(
            instanceProperties,
            view,
            partitions,
            Some(sql),
            None
        )
    }

    private def viewRelationFromTables(executor: Execution) : HiveViewRelation = {
        val tables = listTables(executor)
        viewRelationFromTables(executor, tables)
    }
    private def viewRelationFromTables(executor: Execution, tables:Seq[TableIdentifier]) : HiveViewRelation = {
        val spark = executor.spark
        val df = tables.map(t => spark.read.table(t.unquotedString))
        val finalSchema = schema.get.sparkSchema.fields ++ partitions.map(_.sparkField)
        val sql = HiveUnionTableRelation.unionSql(df, StructType(finalSchema))
        viewRelationFromSql(sql)
    }

    /**
      * Returns the list of all resources which will be created by this relation.
      *
      * @return
      */
    override def provides(op:Operation, partitions:Map[String,FieldValue] = Map.empty): Set[ResourceIdentifier] = {
        op match {
            case Operation.CREATE | Operation.DESTROY =>
                Set(
                    ResourceIdentifier.ofHiveTable(tableRegex),
                    ResourceIdentifier.ofHiveTable(viewIdentifier)
                )
            case Operation.READ => Set.empty
            case Operation.WRITE =>
                requireValidPartitionKeys(partitions)

                // Only return Hive table partitions!
                val allPartitions = PartitionSchema(this.partitions).interpolate(partitions)
                allPartitions.map(p => ResourceIdentifier.ofHivePartition(tableRegex, p.toMap)).toSet
        }
    }

    /**
      * Returns the list of all resources which will be required by this relation for creation.
      *
      * @return
      */
    override def requires(op:Operation, partitions:Map[String,FieldValue] = Map.empty): Set[ResourceIdentifier] = {
        val deps = op match {
            case Operation.CREATE | Operation.DESTROY =>
                tablePrefix.database.map(db => ResourceIdentifier.ofHiveDatabase(db)).toSet ++
                    viewIdentifier.database.map(db => ResourceIdentifier.ofHiveDatabase(db)).toSet
            case Operation.READ =>
                requireValidPartitionKeys(partitions)

                // Only return Hive table partitions!
                val allPartitions = PartitionSchema(this.partitions).interpolate(partitions)
                allPartitions.map(p => ResourceIdentifier.ofHivePartition(tableRegex, p.toMap)).toSet ++
                    Set(
                        ResourceIdentifier.ofHiveTable(tableRegex),
                        ResourceIdentifier.ofHiveTable(viewIdentifier)
                    )
            case Operation.WRITE =>
                Set(
                    ResourceIdentifier.ofHiveTable(tableRegex),
                    ResourceIdentifier.ofHiveTable(viewIdentifier)
                )
        }
        deps ++ super.requires(op, partitions)
    }

    /**
      * Reads data from the relation, possibly from specific partitions
      *
      * @param execution
      * @param partitions - List of partitions. If none are specified, all the data will be read
      * @return
      */
    override def read(execution: Execution, partitions: Map[String, FieldValue]): DataFrame = {
        require(execution != null)
        require(partitions != null)

        logger.info(s"Reading from Hive union relation '$identifier' from UNION VIEW $viewIdentifier using partition values $partitions")

        val tableDf = execution.spark.read.table(viewIdentifier.unquotedString)
        val filteredDf = filterPartition(tableDf, partitions)

        applyInputSchema(execution, filteredDf)
    }

    /**
      * Writes data into the relation, possibly into a specific partition
      *
      * @param execution
      * @param df        - dataframe to write
      * @param partition - destination partition
      */
    override def write(execution: Execution, df: DataFrame, partition: Map[String, SingleValue], mode: OutputMode): Unit = {
        require(execution != null)

        requireAllPartitionKeys(partition)

        val catalog = execution.catalog
        val partitionSchema = PartitionSchema(this.partitions)
        val partitionSpec = partitionSchema.spec(partition)
        logger.info(s"Writing to Hive union relation '$identifier' using partition values ${HiveDialect.expr.partition(partitionSpec)}")

        // 1. Find all tables
        val allTables = listTables(execution)

        // 2. Find appropriate table
        val table = allTables.find { id =>
                val table = catalog.getTable(id)
                SchemaUtils.isCompatible(df.schema, table.schema)
            }
            .orElse {
                // Try to use provided schema instead
                schema.flatMap { schema =>
                    val catalogSchema = schema.catalogSchema
                    allTables.find { id =>
                        val table = catalog.getTable(id)
                        SchemaUtils.isCompatible(catalogSchema, table.schema)
                    }
                }
            }
            .getOrElse {
                logger.error(s"Cannot find appropriate target table for Hive Union Table '$identifier'. Required schema is\n ${df.schema.treeString}")
                throw new ExecutionException(s"Cannot find appropriate target table for Hive Union Table '$identifier'")
            }

        // 3. Drop  partition from all other tables
        if (mode == OutputMode.OVERWRITE) {
            allTables.filter(_ != table).foreach { table =>
                catalog.dropPartition(table, partitionSpec, ignoreIfNotExists = true)
            }
        }

        // 4. Write to that table
        val relation = tableRelation(table, None)
        relation.write(execution, df, partition, mode)
    }

    /**
      * Removes one or more partitions.
      *
      * @param execution
      * @param partitions
      */
    override def truncate(execution: Execution, partitions: Map[String, FieldValue]): Unit = {
        require(execution != null)
        require(partitions != null)

        logger.info(s"Truncating Hive union relation '$identifier' partition $partitions")

        listTables(execution)
            .foreach { table =>
                val relation = tableRelation(table, None)
                relation.truncate(execution, partitions)
            }
    }


    /**
     * Returns true if the target partition exists and contains valid data. Absence of a partition indicates that a
     * [[write]] is required for getting up-to-date contents. A [[write]] with output mode
     * [[OutputMode.ERROR_IF_EXISTS]] then should not throw an error but create the corresponding partition
     *
     * @param execution
     * @param partition
     * @return
     */
    override def loaded(execution: Execution, partition: Map[String, SingleValue]): Trilean = {
        require(execution != null)
        require(partition != null)

        requireValidPartitionKeys(partition)

        val catalog = execution.catalog

        if (this.partitions.isEmpty) {
            catalog.tableExists(viewIdentifier) &&
                listTables(execution).exists { table =>
                    val location = catalog.getTableLocation(table)
                    val fs = location.getFileSystem(execution.hadoopConf)
                    FileUtils.isValidHiveData(fs, location)
                }
        }
        else {
            val partitionSchema = PartitionSchema(this.partitions)
            val partitionSpec = partitionSchema.spec(partition)

            catalog.tableExists(viewIdentifier) &&
                listTables(execution).exists { table =>
                    catalog.partitionExists(table, partitionSpec)
                }
        }
    }

    /**
      * Returns true if the relation already exists, otherwise it needs to be created prior usage
      *
      * @param execution
      * @return
      */
    override def exists(execution: Execution): Trilean = {
        require(execution != null)

        val catalog = execution.catalog
        catalog.tableExists(viewIdentifier)
    }


    /**
     * Returns true if the relation exists and has the correct schema. If the method returns false, but the
     * relation exists, then a call to [[migrate]] should result in a conforming relation.
     *
     * @param execution
     * @return
     */
    override def conforms(execution: Execution, migrationPolicy: MigrationPolicy): Trilean = {
        val catalog = execution.catalog
        if (catalog.tableExists(viewIdentifier)) {
            val catalog = execution.catalog
            val sourceSchema = schema.get.catalogSchema
            val allTables = listTables(execution)

            // 1. Find all tables
            // 2. Find appropriate table
            val target = allTables
                .find { id =>
                    val table = catalog.getTable(id)
                    val targetSchema = table.dataSchema
                    val targetFieldsByName = MapIgnoreCase(targetSchema.map(f => (f.name, f)))

                    sourceSchema.forall { field =>
                        targetFieldsByName.get(field.name)
                            .forall(tgt => SchemaUtils.isCompatible(field, tgt))
                    }
                }

            val needsMigration = target match {
                case Some(id) =>
                    // 3. If found:
                    //  3.1 Migrate table (add new columns)
                    val table = catalog.getTable(id)
                    val targetSchema = table.dataSchema
                    val targetFields = SetIgnoreCase(targetSchema.map(f => f.name))

                    val missingFields = sourceSchema.filterNot(f => targetFields.contains(f.name))
                    if (missingFields.nonEmpty) {
                        true
                    }
                    else {
                        false
                    }

                case None =>
                    // 3. If not found:
                    //  3.2 Create new table
                    true
            }

            if (needsMigration) {
                No
            }
            else {
                val hiveViewRelation = viewRelationFromTables(execution)
                hiveViewRelation.conforms(execution, MigrationPolicy.RELAXED)
            }
        } else {
            false
        }
    }

    /**
      * This method will physically create the corresponding relation. This might be a Hive table or a directory. The
      * relation will not contain any data, but all metadata will be processed
      *
      * @param execution
      */
    override def create(execution: Execution, ifNotExists: Boolean): Unit = {
        require(execution != null)

        if (!ifNotExists || exists(execution) == No) {
            logger.info(s"Creating Hive union relation '$identifier'")
            if (schema.isEmpty) {
                throw new UnspecifiedSchemaException(identifier)
            }

            // Create first table using current schema
            val hiveTableRelation = tableRelation(1)
            hiveTableRelation.create(execution, ifNotExists)

            // Create initial view
            val hiveViewRelation = viewRelationFromTables(execution, Seq(hiveTableRelation.table))
            hiveViewRelation.create(execution, ifNotExists)
        }
    }

    /**
      * This will delete any physical representation of the relation. Depending on the type only some meta data like
      * a Hive table might be dropped or also the physical files might be deleted
      *
      * @param execution
      */
    override def destroy(execution: Execution, ifExists: Boolean): Unit = {
        require(execution != null)

        if (!ifExists || exists(execution) == Yes) {
            val catalog = execution.catalog

            // Destroy view
            logger.info(s"Dropping Hive union relation '$identifier' UNION VIEW $viewIdentifier")
            catalog.dropView(viewIdentifier, ifExists)

            // Destroy tables
            listTables(execution)
                .foreach { table =>
                    logger.info(s"Dropping Hive union relation '$identifier' backend table '$table'")
                    catalog.dropTable(table, false)
                }
        }
    }

    /**
      * This will update any existing relation to the specified metadata.
      *
      * @param execution
      */
    override def migrate(execution: Execution, migrationPolicy:MigrationPolicy, migrationStrategy:MigrationStrategy): Unit = {
        require(execution != null)

        val catalog = execution.catalog
        val sourceSchema = schema.get.catalogSchema
        val allTables = listTables(execution)

        // 1. Find all tables
        // 2. Find appropriate table
        val target = allTables
            .find { id =>
                val table = catalog.getTable(id)
                val targetSchema = table.dataSchema
                val targetFieldsByName = MapIgnoreCase(targetSchema.map(f => (f.name, f)))

                sourceSchema.forall { field =>
                    targetFieldsByName.get(field.name)
                        .forall(tgt => SchemaUtils.isCompatible(field, tgt))
                }
            }

        target match {
            case Some(id) =>
                // 3. If found:
                //  3.1 Migrate table (add new columns)
                val table = catalog.getTable(id)
                val targetSchema = table.dataSchema
                val targetFields = SetIgnoreCase(targetSchema.map(f => f.name))

                val missingFields = sourceSchema.filterNot(f => targetFields.contains(f.name))
                if (missingFields.nonEmpty) {
                    doMigrateAlterTable(execution, table, missingFields, migrationStrategy)
                }

            case None =>
                // 3. If not found:
                //  3.2 Create new table
                doMigrateNewTable(execution, allTables, migrationStrategy)
        }

        //  4 Always migrate union view, maybe SQL generator changed
        val hiveViewRelation = viewRelationFromTables(execution)
        hiveViewRelation.migrate(execution, MigrationPolicy.RELAXED, MigrationStrategy.ALTER)
    }

    private def doMigrate(migrationStrategy:MigrationStrategy)(alter: => Unit) : Unit = {
        migrationStrategy match {
            case MigrationStrategy.NEVER =>
                logger.warn(s"Migration required for HiveUnionTable relation '$identifier' of Hive union table $viewIdentifier, but migrations are disabled.")
            case MigrationStrategy.FAIL =>
                logger.error(s"Cannot migrate HiveUnionTable HiveTable '$identifier' of Hive union table $viewIdentifier, since migrations are disabled.")
                throw new MigrationFailedException(identifier)
            case MigrationStrategy.ALTER|MigrationStrategy.ALTER_REPLACE =>
                alter
            case MigrationStrategy.REPLACE =>
                logger.warn(s"Migration required for HiveUnionTable relation '$identifier' of Hive union table $viewIdentifier with migration strategy ${MigrationStrategy.REPLACE}, altering table instead.")
                alter
        }
    }

    private def doMigrateAlterTable(execution:Execution, table:CatalogTable, missingFields:Seq[StructField], migrationStrategy:MigrationStrategy) : Unit = {
        doMigrate(migrationStrategy) {
            val catalog = execution.catalog
            val id = TableIdentifier.of(table.identifier)
            val targetSchema = table.dataSchema
            val newSchema = StructType(targetSchema.fields ++ missingFields)
            logger.info(s"Migrating Hive Union Table relation '$identifier' by adding new columns ${missingFields.map(_.name).mkString(",")} to Hive table $id. New schema is\n ${newSchema.treeString}")
            catalog.addTableColumns(id, missingFields)
        }
    }

    private def doMigrateNewTable(execution:Execution, allTables:Seq[TableIdentifier], migrationStrategy:MigrationStrategy) : Unit = {
        doMigrate(migrationStrategy) {
            val tableSet = allTables.toSet
            val version = (1 to 100000).find(n => !tableSet.contains(tableIdentifier(n))).get
            logger.info(s"Migrating Hive Union Table relation '$identifier' by creating new Hive table ${tableIdentifier(version)}")
            val hiveTableRelation = tableRelation(version)
            hiveTableRelation.create(execution, false)
        }
    }
}




class HiveUnionTableRelationSpec extends RelationSpec with SchemaRelationSpec with PartitionedRelationSpec {
    @JsonProperty(value = "tableDatabase", required = false) private var tableDatabase: Option[String] = None
    @JsonProperty(value = "tablePrefix", required = true) private var tablePrefix: String = "zz"
    @JsonProperty(value = "locationPrefix", required = false) private var locationPrefix: Option[String] = None
    @JsonProperty(value = "viewDatabase", required = false) private var viewDatabase: Option[String] = None
    @JsonProperty(value = "view", required = true) private var view: String = ""
    @JsonProperty(value = "external", required = false) private var external: String = "false"
    @JsonProperty(value = "format", required = false) private var format: Option[String] = None
    @JsonProperty(value = "options", required=false) private var options:Map[String,String] = Map()
    @JsonProperty(value = "rowFormat", required = false) private var rowFormat: Option[String] = None
    @JsonProperty(value = "inputFormat", required = false) private var inputFormat: Option[String] = None
    @JsonProperty(value = "outputFormat", required = false) private var outputFormat: Option[String] = None
    @JsonProperty(value = "properties", required = false) private var properties: Map[String, String] = Map()
    @JsonProperty(value = "serdeProperties", required = false) private var serdeProperties: Map[String, String] = Map()

    /**
      * Creates the instance of the specified Relation with all variable interpolation being performed
      * @param context
      * @return
      */
    override def instantiate(context: Context, props:Option[Relation.Properties] = None): HiveUnionTableRelation = {
        HiveUnionTableRelation(
            instanceProperties(context, props),
            schema.map(_.instantiate(context)),
            partitions.map(_.instantiate(context)),
            TableIdentifier(context.evaluate(tablePrefix), context.evaluate(tableDatabase)),
            context.evaluate(locationPrefix).map(p => new Path(context.evaluate(p))),
            TableIdentifier(context.evaluate(view), context.evaluate(viewDatabase)),
            context.evaluate(external).toBoolean,
            context.evaluate(format),
            context.evaluate(options),
            context.evaluate(rowFormat),
            context.evaluate(inputFormat),
            context.evaluate(outputFormat),
            context.evaluate(properties),
            context.evaluate(serdeProperties)
        )
    }
}
