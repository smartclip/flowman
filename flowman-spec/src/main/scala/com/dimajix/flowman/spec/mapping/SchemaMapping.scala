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

import scala.collection.immutable.ListMap

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import org.apache.spark.sql.DataFrame

import com.dimajix.jackson.ListMapDeserializer

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.model.BaseMapping
import com.dimajix.flowman.model.Mapping
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.model.Schema
import com.dimajix.flowman.spec.schema.SchemaSpec
import com.dimajix.flowman.transforms.ColumnMismatchPolicy
import com.dimajix.flowman.transforms.SchemaEnforcer
import com.dimajix.flowman.transforms.CharVarcharPolicy
import com.dimajix.flowman.transforms.TypeMismatchPolicy
import com.dimajix.flowman.types.Field
import com.dimajix.flowman.types.FieldType
import com.dimajix.flowman.types.StructType
import com.dimajix.spark.sql.DataFrameBuilder


case class SchemaMapping(
    instanceProperties:Mapping.Properties,
    input:MappingOutputIdentifier,
    columns:Seq[Field] = Seq(),
    schema:Option[Schema] = None,
    columnMismatchPolicy:ColumnMismatchPolicy = ColumnMismatchPolicy.ADD_REMOVE_COLUMNS,
    typeMismatchPolicy:TypeMismatchPolicy = TypeMismatchPolicy.CAST_ALWAYS,
    charVarcharPolicy:CharVarcharPolicy = CharVarcharPolicy.PAD_AND_TRUNCATE,
    filter:Option[String] = None
)
extends BaseMapping {
    if (schema.isEmpty && columns.isEmpty)
        throw new IllegalArgumentException(s"Require either schema or columns in mapping $name")

    /**
     * Returns the dependencies of this mapping, which is exactly one input table
     *
     * @return
     */
    override def inputs : Set[MappingOutputIdentifier] = {
        Set(input) ++ expressionDependencies(filter)
    }

    /**
      * Executes this MappingType and returns a corresponding DataFrame
      *
      * @param execution
      * @param tables
      * @return
      */
    override def execute(execution:Execution, tables:Map[MappingOutputIdentifier,DataFrame]) : Map[String,DataFrame] = {
        require(execution != null)
        require(tables != null)

        val df = tables(input)
        val result = xfs.transform(df)

        // Apply optional filter
        val filteredResult = applyFilter(result, filter, tables)

        Map("main" -> filteredResult)
    }

    /**
      * Returns the schema as produced by this mapping, relative to the given input schema
      * @param input
      * @return
      */
    override def describe(execution: Execution, input:Map[MappingOutputIdentifier,StructType]) : Map[String,StructType] = {
        require(execution != null)
        require(input != null)

        val df = DataFrameBuilder.singleRow(execution.spark, input(this.input).sparkType)
        val result = StructType.of(xfs.transform(df).schema)

        // Apply documentation
        val schemas = Map("main" -> result)
        applyDocumentation(schemas)
    }

    private lazy val xfs = if(schema.nonEmpty) {
        SchemaEnforcer(schema.get.catalogSchema)
    }
    else {
        SchemaEnforcer(StructType(columns).catalogType)
    }
}



class SchemaMappingSpec extends MappingSpec {
    @JsonProperty(value = "input", required = true) private var input: String = _
    @JsonDeserialize(using = classOf[ListMapDeserializer]) // Old Jackson in old Spark doesn't support ListMap
    @JsonProperty(value = "columns", required = false) private var columns: ListMap[String,String] = ListMap()
    @JsonProperty(value = "schema", required = false) private var schema: Option[SchemaSpec] = None
    @JsonProperty(value = "filter", required=false) private var filter:Option[String] = None
    @JsonProperty(value = "columnMismatchPolicy", required=false) private var columnMismatchPolicy:String = "ADD_REMOVE_COLUMNS"
    @JsonProperty(value = "typeMismatchPolicy", required=false) private var typeMismatchPolicy:String = "CAST_ALWAYS"
    @JsonProperty(value = "charVarcharPolicy", required=false) private var charVarcharPolicy:String = "PAD_AND_TRUNCATE"

    /**
      * Creates the instance of the specified Mapping with all variable interpolation being performed
      * @param context
      * @return
      */
    override def instantiate(context: Context, properties:Option[Mapping.Properties] = None): SchemaMapping = {
        SchemaMapping(
            instanceProperties(context, properties),
            MappingOutputIdentifier(context.evaluate(this.input)),
            columns.toSeq.map(kv => Field(kv._1, FieldType.of(context.evaluate(kv._2)))),
            schema.map(_.instantiate(context)),
            ColumnMismatchPolicy.ofString(context.evaluate(columnMismatchPolicy)),
            TypeMismatchPolicy.ofString(context.evaluate(typeMismatchPolicy)),
            CharVarcharPolicy.ofString(context.evaluate(charVarcharPolicy)),
            context.evaluate(filter)
        )
    }
}
