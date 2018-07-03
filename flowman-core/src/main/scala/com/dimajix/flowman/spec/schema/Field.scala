/*
 * Copyright 2018 Kaya Kupferschmidt
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

package com.dimajix.flowman.spec.schema

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.MetadataBuilder
import org.apache.spark.sql.types.StructField

import com.dimajix.flowman.execution.Context


object Field {
    def apply(name:String, ftype:FieldType, nullable:Boolean=true, description:String=null, default:String=null) : Field = {
        val field = new Field()
        field._name = name
        field._type = ftype
        field._nullable = nullable.toString
        field._description = description
        field._default = default
        field
    }
}

/**
  * A Field represents a single entry in a Schema or a Struct.
  */
class Field {
    @JsonProperty(value="name", required = true) private var _name: String = _
    @JsonProperty(value="type", required = false) private var _type: FieldType = _
    @JsonProperty(value="nullable", required = true) private var _nullable: String = "true"
    @JsonProperty(value="description", required = false) private var _description: String = _
    @JsonProperty(value="size", required = false) private var _size: String = _
    @JsonProperty(value="default", required = false) private var _default: String = _

    def name : String = _name
    def ftype : FieldType = _type
    def nullable : Boolean = _nullable.toBoolean
    def description : String = _description
    def size : Int = Option(_size).map(_.trim).filter(_.nonEmpty).map(_.toInt).getOrElse(0)
    def default : String = _default

    /**
      * Returns an appropriate (Hive) SQL type for this field. These can be directly used in CREATE TABLE statements.
      * The SQL type might also be complex, for example in the case of StructTypes
      * @return
      */
    def sqlType : String = _type.sqlType

    /**
      * Returns an appropriate type name to be used for pretty printing the schema as a tree. Struct types will not
      * be resolved
      * @return
      */
    def typeName : String = _type.typeName

    /**
      * Returns the Spark data type used for this field
      * @return
      */
    def sparkType : DataType = _type.sparkType

    /**
      * Converts the field into a Spark field including metadata containing the fields description and size
      * @return
      */
    def sparkField : StructField = {
        val metadata = new MetadataBuilder()
        Option(description).map(_.trim).filter(_.nonEmpty).foreach(d => metadata.putString("comment", d))
        Option(size).filter(_ > 0).foreach(s => metadata.putLong("size", s))
        StructField(name, sparkType, nullable, metadata.build())
    }
}