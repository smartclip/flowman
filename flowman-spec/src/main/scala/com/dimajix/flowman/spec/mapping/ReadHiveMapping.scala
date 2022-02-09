/*
 * Copyright 2021 Kaya Kupferschmidt
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

package com.dimajix.flowman.spec.mapping

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.spark
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.TableIdentifier
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.model.BaseMapping
import com.dimajix.flowman.model.Mapping
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.model.ResourceIdentifier
import com.dimajix.flowman.types.Field
import com.dimajix.flowman.types.FieldType
import com.dimajix.flowman.types.StructType
import com.dimajix.spark.sql.SchemaUtils


case class ReadHiveMapping(
    instanceProperties:Mapping.Properties,
    database: Option[String] = None,
    table: String,
    columns:Seq[Field] = Seq(),
    filter:Option[String] = None
)
extends BaseMapping {
    private val logger = LoggerFactory.getLogger(classOf[ReadHiveMapping])

    def tableIdentifier: TableIdentifier = new TableIdentifier(table, database)

    /**
     * Returns a list of physical resources required by this mapping. This list will only be non-empty for mappings
     * which actually read from physical data.
     * @return
     */
    override def requires : Set[ResourceIdentifier] = {
        Set(ResourceIdentifier.ofHiveTable(table, database)) ++
            database.map(db => ResourceIdentifier.ofHiveDatabase(db)).toSet
    }

    /**
     * Returns the dependencies of this mapping, which are empty for an ReadRelationMapping
     *
     * @return
     */
    override def inputs : Seq[MappingOutputIdentifier] = {
        Seq()
    }

    /**
     * Executes this Transform by reading from the specified source and returns a corresponding DataFrame
     *
     * @param execution
     * @param input
     * @return
     */
    override def execute(execution:Execution, input:Map[MappingOutputIdentifier,DataFrame]): Map[String,DataFrame] = {
        require(execution != null)
        require(input != null)

        val schema = if (columns.nonEmpty) Some(spark.sql.types.StructType(columns.map(_.sparkField))) else None
        logger.info(s"Reading Hive table $tableIdentifier with filter '${filter.getOrElse("")}'")

        val reader = execution.spark.read
        val tableDf = reader.table(tableIdentifier.unquotedString)
        val df = SchemaUtils.applySchema(tableDf, schema)

        // Apply optional filter
        val result = filter.map(df.filter).getOrElse(df)

        Map("main" -> result)
    }

    /**
     * Returns the schema as produced by this mapping, relative to the given input schema
     * @param input
     * @return
     */
    override def describe(execution:Execution, input:Map[MappingOutputIdentifier,StructType]) : Map[String,StructType] = {
        require(execution != null)
        require(input != null)

        val result = if (columns.nonEmpty) {
            // Use user specified schema
            StructType(columns)
        }
        else {
            val tableDf = execution.spark.read.table(tableIdentifier.unquotedString)
            StructType.of(tableDf.schema)
        }

        // Apply documentation
        val schemas = Map("main" -> result)
        applyDocumentation(schemas)
    }
}


class ReadHiveMappingSpec extends MappingSpec {
    @JsonProperty(value = "database", required = false) private var database: Option[String] = None
    @JsonProperty(value = "table", required = true) private var table: String = ""
    @JsonProperty(value = "columns", required=false) private var columns:Map[String,String] = Map()
    @JsonProperty(value = "filter", required=false) private var filter:Option[String] = None

    /**
     * Creates the instance of the specified Mapping with all variable interpolation being performed
     * @param context
     * @return
     */
    override def instantiate(context: Context): ReadHiveMapping = {
        ReadHiveMapping(
            instanceProperties(context),
            context.evaluate(database),
            context.evaluate(table),
            context.evaluate(columns).map { case(name,typ) => Field(name, FieldType.of(typ))}.toSeq,
            context.evaluate(filter)
        )
    }
}
