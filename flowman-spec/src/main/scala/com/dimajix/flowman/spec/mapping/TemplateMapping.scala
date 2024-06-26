/*
 * Copyright 2019-2022 Kaya Kupferschmidt
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
import org.apache.spark.sql.DataFrame

import com.dimajix.flowman.common.ParserUtils.splitSettings
import com.dimajix.flowman.documentation.MappingDoc
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.execution.ScopeContext
import com.dimajix.flowman.graph.Linker
import com.dimajix.flowman.model.BaseMapping
import com.dimajix.flowman.model.Mapping
import com.dimajix.flowman.model.MappingIdentifier
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.model.Metadata
import com.dimajix.flowman.model.ResourceIdentifier
import com.dimajix.flowman.types.StructType


case class TemplateMapping(
    instanceProperties:Mapping.Properties,
    mapping:MappingIdentifier,
    environment:Map[String,String],
    filter:Option[String] = None
) extends BaseMapping {
    private val templateContext = ScopeContext.builder(context)
        .withEnvironment(environment)
        .build()
    lazy val mappingInstance : Mapping = {
        val meta = Metadata(templateContext, name, category, "")
        val props = Mapping.Properties(templateContext, meta, super.broadcast, super.checkpoint, super.cache, super.documentation)
        val spec = project.get.mappings(mapping.name)
        spec.instantiate(templateContext, Some(props))
    }

    /**
     * Returns a (static) documentation of this mapping
     *
     * @return
     */
    override def documentation: Option[MappingDoc] = {
        mappingInstance.documentation
            .map(_.copy(mapping=Some(this)))
    }

    /**
      * Returns a list of physical resources required by this mapping. This list will only be non-empty for mappings
      * which actually read from physical data.
      *
      * @return
      */
    override def requires: Set[ResourceIdentifier] = mappingInstance.requires

    /**
      * Lists all outputs of this mapping. Every mapping should have one "main" output
 *
      * @return
      */
    override def outputs : Set[String] = mappingInstance.outputs

    /**
      * Returns the dependencies (i.e. names of tables in the Dataflow model)
      *
      * @return
      */
    override def inputs: Set[MappingOutputIdentifier] = mappingInstance.inputs ++ expressionDependencies(filter)

    /**
      * Executes this MappingType and returns a corresponding DataFrame
      *
      * @param execution
      * @param input
      * @return
      */
    override def execute(execution: Execution, input: Map[MappingOutputIdentifier, DataFrame]) : Map[String,DataFrame] = {
        require(execution != null)
        require(input != null)

        val result = mappingInstance.execute(execution, input)

        // Apply optional filter
        result.map { case(name,df) => name -> applyFilter(df, filter, input) }
    }

    /**
      * Returns the schema as produced by this mapping, relative to the given input schema
      * @param input
      * @return
      */
    override def describe(execution:Execution, input:Map[MappingOutputIdentifier,StructType]) : Map[String,StructType] = {
        require(execution != null)
        require(input != null)

        mappingInstance.describe(execution, input)
    }

    /**
     * Creates all known links for building a descriptive graph of the whole data flow
     * Params: linker - The linker object to use for creating new edges
     */
    override def link(linker: Linker): Unit = mappingInstance.link(linker)
}



class TemplateMappingSpec extends MappingSpec {
    @JsonProperty(value = "mapping", required = true) private var mapping:String = _
    @JsonProperty(value = "environment", required = true) private var environment:Seq[String] = Seq()
    @JsonProperty(value = "filter", required=false) private var filter:Option[String] = None

    /**
      * Creates an instance of this specification and performs the interpolation of all variables
      *
      * @param context
      * @return
      */
    override def instantiate(context: Context, properties:Option[Mapping.Properties] = None): TemplateMapping = {
        TemplateMapping(
            instanceProperties(context, properties),
            MappingIdentifier(context.evaluate(mapping)),
            splitSettings(environment).toMap,
            context.evaluate(filter)
        )
    }
}
