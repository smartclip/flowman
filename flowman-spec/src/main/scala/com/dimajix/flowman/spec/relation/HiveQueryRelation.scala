/*
 * Copyright 2022 Kaya Kupferschmidt
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

import java.io.StringWriter
import java.nio.charset.Charset

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonPropertyDescription
import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory

import com.dimajix.common.Trilean
import com.dimajix.common.Yes
import com.dimajix.flowman.catalog.TableIdentifier
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.execution.MergeClause
import com.dimajix.flowman.execution.MigrationPolicy
import com.dimajix.flowman.execution.MigrationStrategy
import com.dimajix.flowman.execution.Operation
import com.dimajix.flowman.execution.OutputMode
import com.dimajix.flowman.model.BaseRelation
import com.dimajix.flowman.model.PartitionField
import com.dimajix.flowman.model.PartitionedRelation
import com.dimajix.flowman.model.Relation
import com.dimajix.flowman.model.ResourceIdentifier
import com.dimajix.flowman.model.Schema
import com.dimajix.flowman.types.FieldValue
import com.dimajix.flowman.types.SingleValue
import com.dimajix.spark.sql.SqlParser



case class HiveQueryRelation(
    override val instanceProperties:Relation.Properties,
    override val schema:Option[Schema] = None,
    override val partitions: Seq[PartitionField] = Seq.empty,
    sql: Option[String] = None,
    file: Option[Path] = None
) extends BaseRelation with PartitionedRelation {
    private val logger = LoggerFactory.getLogger(classOf[HiveQueryRelation])

    /**
     * Returns the list of all resources which will be created by this relation.
     *
     * @return
     */
    override def provides(op:Operation, partitions:Map[String,FieldValue] = Map.empty): Set[ResourceIdentifier] = {
        op match {
            case Operation.CREATE | Operation.DESTROY =>
                Set.empty
            case Operation.READ =>
                Set.empty
            case Operation.WRITE => Set.empty
        }
    }

    /**
     * Returns the list of all resources which will be required by this relation for creation.
     *
     * @return
     */
    override def requires(op:Operation, partitions:Map[String,FieldValue] = Map.empty) : Set[ResourceIdentifier] = {
        op match {
            case Operation.CREATE | Operation.DESTROY =>
                dependencies.map(l => ResourceIdentifier.ofHiveTable(TableIdentifier(l)))
            case Operation.READ =>
                dependencies.flatMap(l => Set(
                    ResourceIdentifier.ofHiveTable(TableIdentifier(l)),
                    ResourceIdentifier.ofHivePartition(TableIdentifier(l), Map.empty[String,Any])
                ))
            case Operation.WRITE => Set.empty
        }
    }

    /**
     * Reads the configured table from the source
     * @param execution
     * @return
     */
    override def read(execution:Execution, partitions:Map[String,FieldValue] = Map()) : DataFrame = {
        require(execution != null)
        require(partitions != null)

        logger.info(s"Reading Hive query relation '$identifier' partition $partitions")

        // Read from database. We do not use this.reader, because Spark JDBC sources do not support explicit schemas
        val tableDf = execution.spark.sql(query)
        val filteredDf = filterPartition(tableDf, partitions)

        // Apply embedded schema, if it is specified. This will remove/cast any columns not present in the
        // explicit schema specification of the relation
        applyInputSchema(execution, filteredDf)
    }

    /**
     * Writes a given DataFrame into a JDBC connection
     *
     * @param execution
     * @param df
     * @param partition
     * @param mode
     */
    override def write(execution:Execution, df:DataFrame, partition:Map[String,SingleValue], mode:OutputMode) : Unit = {
        throw new UnsupportedOperationException(s"Cannot write into Hive query relation '$identifier' which is defined by an SQL query")
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
        throw new UnsupportedOperationException(s"Cannot write into Hive query relation '$identifier' which is defined by an SQL query")
    }

    /**
     * Removes one or more partitions.
     *
     * @param execution
     * @param partitions
     */
    override def truncate(execution: Execution, partitions: Map[String, FieldValue]): Unit = {
        throw new UnsupportedOperationException(s"Cannot clean Hive query relation '$identifier' which is defined by an SQL query")
    }

    /**
     * Returns true if the relation already exists, otherwise it needs to be created prior usage
     * @param execution
     * @return
     */
    override def exists(execution:Execution) : Trilean = {
        require(execution != null)
        true
    }

    /**
     * Returns true if the relation exists and has the correct schema. If the method returns false, but the
     * relation exists, then a call to [[migrate]] should result in a conforming relation.
     *
     * @param execution
     * @return
     */
    override def conforms(execution: Execution, migrationPolicy: MigrationPolicy): Trilean = {
        true
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

        Yes
    }

    override def create(execution:Execution, ifNotExists:Boolean=false) : Unit = {
        throw new UnsupportedOperationException(s"Cannot create Hive query relation '$identifier' which is defined by an SQL query")
    }

    override def destroy(execution:Execution, ifExists:Boolean=false) : Unit = {
        throw new UnsupportedOperationException(s"Cannot destroy Hive query relation '$identifier' which is defined by an SQL query")
    }

    override def migrate(execution:Execution, migrationPolicy:MigrationPolicy, migrationStrategy:MigrationStrategy) : Unit = {
        throw new UnsupportedOperationException(s"Cannot migrate Hive query relation '$identifier' which is defined by an SQL query")
    }

    private lazy val dependencies : Set[String] = {
        SqlParser.resolveDependencies(query)
    }

    private lazy val query : String = {
        sql
            .orElse(file.map { f =>
                val fs = context.fs
                val input = fs.file(f).open()
                try {
                    val writer = new StringWriter()
                    IOUtils.copy(input, writer, Charset.forName("UTF-8"))
                    writer.toString
                }
                finally {
                    input.close()
                }
            })
            .getOrElse(
                throw new IllegalArgumentException("Require either 'sql' or 'file' property")
            )
    }
}



class HiveQueryRelationSpec extends RelationSpec with PartitionedRelationSpec with SchemaRelationSpec {
    @JsonPropertyDescription("SQL query for the view definition. This has to be specified in Spark SQL syntax.")
    @JsonProperty(value="sql", required = false) private var sql: Option[String] = None
    @JsonPropertyDescription("Name of a file containing the SQL query for the view definition. This has to be specified in Spark SQL syntax.")
    @JsonProperty(value="file", required=false) private var file:Option[String] = None

    /**
     * Creates the instance of the specified Relation with all variable interpolation being performed
     * @param context
     * @return
     */
    override def instantiate(context: Context, properties:Option[Relation.Properties] = None): HiveQueryRelation = {
        HiveQueryRelation(
            instanceProperties(context, properties),
            schema.map(_.instantiate(context)),
            partitions.map(_.instantiate(context)),
            context.evaluate(sql),
            context.evaluate(file).map(p => new Path(p))
        )
    }
}
