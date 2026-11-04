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

package com.dimajix.flowman.execution

import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

import com.dimajix.flowman.types.StructType


/**
  * No-op variant of the Flowman Spark Connect server plugin gateway for Spark 3.x builds. Spark Connect client
  * internals used by the real implementation (see the "spark-connect" source directory) are not available there,
  * and the server plugin requires Spark 4.x anyway, so all plugin features report themselves as unavailable.
  */
object ConnectServerPlugin {
    final case class Capabilities(flowmanVersion:String, apiVersion:String)

    def capabilities(spark:SparkSession) : Option[Capabilities] = None

    def isAvailable(spark:SparkSession) : Boolean = false

    def describe(execution:Execution, df:DataFrame) : StructType = StructType.of(df.schema)

    def nullableStruct(spark:SparkSession, cols:Seq[Column]) : Option[Column] = None

    def countRecords(spark:SparkSession, df:DataFrame) : Option[DataFrame] = None

    def relationSupportsMultiplePaths(spark:SparkSession, format:String) : Option[Boolean] = None

    def ping(spark:SparkSession) : Boolean = false
}
