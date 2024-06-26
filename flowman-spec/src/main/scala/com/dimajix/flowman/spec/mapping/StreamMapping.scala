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

package com.dimajix.flowman.spec.mapping

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.spark
import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory

import com.dimajix.common.MapIgnoreCase
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.execution.Operation
import com.dimajix.flowman.graph.Linker
import com.dimajix.flowman.graph.MappingRef
import com.dimajix.flowman.model.BaseMapping
import com.dimajix.flowman.model.Mapping
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.model.Reference
import com.dimajix.flowman.model.Relation
import com.dimajix.flowman.model.RelationIdentifier
import com.dimajix.flowman.model.ResourceIdentifier
import com.dimajix.flowman.spec.relation.RelationReferenceSpec
import com.dimajix.flowman.types.Field
import com.dimajix.flowman.types.FieldType
import com.dimajix.flowman.types.FieldValue
import com.dimajix.flowman.types.StructType
import com.dimajix.spark.sql.SchemaUtils


case class StreamMapping (
    instanceProperties:Mapping.Properties,
    relation:Reference[Relation],
    columns:Seq[Field],
    filter:Option[String] = None
) extends BaseMapping {
    private val logger = LoggerFactory.getLogger(classOf[StreamMapping])

    /**
     * Returns a list of physical resources required by this mapping. This list will only be non-empty for mappings
     * which actually read from physical data.
     * @return
     */
    override def requires : Set[ResourceIdentifier] = {
        val rel = relation.value
        rel.requires(Operation.READ)
    }

    /**
     * Returns the dependencies of this mapping, which are empty for an InputMapping
     *
     * @return
     */
    override def inputs : Set[MappingOutputIdentifier] = expressionDependencies(filter)

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
        logger.info(s"Reading from streaming relation '${relation.identifier}' with filter '${filter.getOrElse("")}'")

        val rel = relation.value
        val df = rel.readStream(execution)

        // Apply optional filter
        val result = applyFilter(df, filter, input)

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
            val relation = this.relation.value
            execution.describe(relation)
        }

        // Apply documentation
        val schemas = Map("main" -> result)
        applyDocumentation(schemas)
    }

    /**
     * Creates all known links for building a descriptive graph of the whole data flow
     * Params: linker - The linker object to use for creating new edges
     */
    override def link(linker: Linker): Unit = {
        val relref = linker.read(relation, Map.empty[String,FieldValue])
        val relFields = MapIgnoreCase(relref.fields.map(f => f.name -> f))

        val mapref = linker.node.asInstanceOf[MappingRef]
        val outref = mapref.outputs.head

        outref.fields.foreach { f =>
            relFields.get(f.name).foreach { in =>
                linker.connect(in, f)
            }
        }
    }
}



class StreamMappingSpec extends MappingSpec {
    @JsonProperty(value = "relation", required = true) private var relation:RelationReferenceSpec = _
    @JsonProperty(value = "columns", required=false) private var columns:Map[String,String] = Map()
    @JsonProperty(value = "filter", required=false) private var filter:Option[String] = None

    /**
      * Creates the instance of the specified Mapping with all variable interpolation being performed
      * @param context
      * @return
      */
    override def instantiate(context: Context, properties:Option[Mapping.Properties] = None): StreamMapping = {
        StreamMapping(
            instanceProperties(context, properties),
            relation.instantiate(context),
            context.evaluate(columns).map { case(name,typ) => Field(name, FieldType.of(typ))}.toSeq,
            context.evaluate(filter)
        )
    }
}
