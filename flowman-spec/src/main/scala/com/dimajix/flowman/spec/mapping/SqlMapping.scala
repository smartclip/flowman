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

import java.io.StringWriter
import java.net.URL
import java.nio.charset.Charset

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonPropertyDescription
import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.model.BaseMapping
import com.dimajix.flowman.model.Mapping
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.spark.sql.DataFrameUtils
import com.dimajix.spark.sql.SqlParser


case class SqlMapping(
     instanceProperties:Mapping.Properties,
     sql:Option[String] = None,
     file:Option[Path] = None,
     url:Option[URL] = None
)
extends BaseMapping {
    /**
      * Executes this MappingType and returns a corresponding DataFrame
      *
      * @param execution
      * @param input
      * @return
      */
    override def execute(execution:Execution, input:Map[MappingOutputIdentifier,DataFrame]) : Map[String,DataFrame] = {
        require(execution != null)
        require(input != null)

        val result = DataFrameUtils.withTempViews(input.map(kv => kv._1.name -> kv._2)) {
            execution.spark.sql(statement)
        }

        Map("main" -> result)
    }

    /**
      * Resolves all dependencies required to build the SQL
      *
      * @return
      */
    override def inputs : Set[MappingOutputIdentifier] = dependencies

    private lazy val dependencies = SqlParser.resolveDependencies(statement).map(MappingOutputIdentifier.parse)
    private lazy val statement : String = {
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
            .orElse(url.map { url =>
                IOUtils.toString(url, "UTF-8")
            })
            .getOrElse(
                throw new IllegalArgumentException("SQL mapping needs either 'sql', 'file' or 'url'")
            )
    }
}



class SqlMappingSpec extends MappingSpec {
    @JsonPropertyDescription("SQL query to execute. This has to be specified in Spark SQL syntax.")
    @JsonProperty(value="sql", required=false) private var sql:Option[String] = None
    @JsonPropertyDescription("Name of a file containing the SQL query to execute. This has to be specified in Spark SQL syntax.")
    @JsonProperty(value="file", required=false) private var file:Option[String] = None
    @JsonPropertyDescription("URL of a file containing the SQL query to execute. This has to be specified in Spark SQL syntax.")
    @JsonProperty(value="url", required=false) private var url: Option[String] = None

    /**
      * Creates the instance of the specified Mapping with all variable interpolation being performed
      * @param context
      * @return
      */
    override def instantiate(context: Context, properties:Option[Mapping.Properties] = None): SqlMapping = {
        SqlMapping(
            instanceProperties(context, properties),
            context.evaluate(sql),
            context.evaluate(file).map(p => new Path(p)),
            context.evaluate(url).map(u => new URL(u))
        )
    }
}
