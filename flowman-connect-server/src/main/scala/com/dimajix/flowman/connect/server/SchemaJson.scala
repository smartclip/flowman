/*
 * Copyright (C) 2026 The Flowman Authors
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

package com.dimajix.flowman.connect.server

import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.MapType
import org.apache.spark.sql.types.Metadata
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType


/**
  * Minimal JSON serializer for Spark schemas, producing the exact JSON format which `org.apache.spark.sql.types.
  * DataType.fromJson` can parse back (see the "type", "fields", "elementType", "keyType"/"valueType" and "metadata"
  * fields). It is used to ship an enriched schema (incl. CHAR/VARCHAR and comment metadata) from the server plugin
  * to the client.
  *
  * Only string, long and boolean metadata values are encoded, which is sufficient for the Flowman metadata keys
  * ("comment", "__CHAR_VARCHAR_TYPE_STRING", "default", "size", "format", "charset", "collation").
  */
private[server] object SchemaJson {
    def encode(struct:StructType) : String = {
        val sb = new StringBuilder
        encodeStruct(sb, struct)
        sb.toString
    }


    private def encodeStruct(sb:StringBuilder, struct:StructType) : Unit = {
        sb.append("{\"type\":\"struct\",\"fields\":[")
        struct.fields.zipWithIndex.foreach { case (field,index) =>
            if (index > 0)
                sb.append(',')
            encodeField(sb, field)
        }
        sb.append("]}")
    }

    private def encodeField(sb:StringBuilder, field:StructField) : Unit = {
        sb.append("{\"name\":")
        encodeString(sb, field.name)
        sb.append(",\"type\":")
        encodeType(sb, field.dataType)
        sb.append(",\"nullable\":").append(field.nullable)
        sb.append(",\"metadata\":")
        encodeMetadata(sb, field.metadata)
        sb.append('}')
    }

    private def encodeType(sb:StringBuilder, dataType:DataType) : Unit = {
        dataType match {
            case struct:StructType => encodeStruct(sb, struct)
            case array:ArrayType =>
                sb.append("{\"type\":\"array\",\"elementType\":")
                encodeType(sb, array.elementType)
                sb.append(",\"containsNull\":").append(array.containsNull)
                sb.append('}')
            case map:MapType =>
                sb.append("{\"type\":\"map\",\"keyType\":")
                encodeType(sb, map.keyType)
                sb.append(",\"valueType\":")
                encodeType(sb, map.valueType)
                sb.append(",\"valueContainsNull\":").append(map.valueContainsNull)
                sb.append('}')
            case _ =>
                encodeString(sb, canonicalTypeName(dataType))
        }
    }

    private def canonicalTypeName(dataType:DataType) : String = {
        // `catalogString` uses the catalog/Legacy names (e.g. "int"), which `DataType.fromJson`
        // does not accept anymore. Parametersed types like "decimal(10,2)" or "char(5)" are
        // handled by `fromJson` directly and are passed through unchanged.
        dataType.catalogString match {
            case "int" => "integer"
            case "bigint" => "long"
            case "tinyint" => "byte"
            case "smallint" => "short"
            case "numeric" => "decimal"
            case "real" => "float"
            case "double precision" => "double"
            case other => other
        }
    }

    private def encodeMetadata(sb:StringBuilder, metadata:Metadata) : Unit = {
        if (metadata.isEmpty)
            sb.append("{}")
        else
            sb.append(metadata.json)
    }

    private def encodeString(sb:StringBuilder, value:String) : Unit = {
        sb.append('"')
        value.foreach {
            case '"' => sb.append("\\\"")
            case '\\' => sb.append("\\\\")
            case '\n' => sb.append("\\n")
            case '\r' => sb.append("\\r")
            case '\t' => sb.append("\\t")
            case '\b' => sb.append("\\b")
            case '\f' => sb.append("\\f")
            case c if c < 0x20 => sb.append(f"\\u${c.toInt}%04x")
            case c => sb.append(c)
        }
        sb.append('"')
    }
}
