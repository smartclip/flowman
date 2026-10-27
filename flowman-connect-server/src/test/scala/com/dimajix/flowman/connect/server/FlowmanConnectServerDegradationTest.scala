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

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

import com.dimajix.flowman.execution.ConnectServerPlugin
import com.dimajix.flowman.execution.FlowmanFunctions


/**
  * Verifies that the Flowman client degrades gracefully when the server plugin is NOT installed (e.g. a plain
  * Spark Connect endpoint). The `configureServerSpark` hook is overridden to prevent the fixture from
  * reflection-registering the plugin classes, and the plugin registry is reset before the session starts.
  */
class FlowmanConnectServerDegradationTest extends FlowmanConnectServerTestBase {
    override def configureServerSpark(builder: SparkSession.Builder) : SparkSession.Builder = builder

    "The Flowman server plugin" should "be absent" in {
        ConnectServerPlugin.isAvailable(spark) should be (false)
        ConnectServerPlugin.capabilities(spark) should be (None)
        ConnectServerPlugin.ping(spark) should be (false)
        ConnectServerPlugin.relationSupportsMultiplePaths(spark, "parquet") should be (None)
    }

    it should "fall back to the public API for nullable_struct" in {
        val schema = StructType(Seq(
            StructField("a", IntegerType, nullable = true),
            StructField("b", IntegerType, nullable = true)
        ))
        val df = spark.createDataFrame(Seq(
            Row(1, null),
            Row(null, 2),
            Row(null, null)
        ).asJava, schema)

        val result = df.select(FlowmanFunctions.nullableStruct(Some(spark), df("a"), df("b")).as("s"))
        val rows = result.collect()
        rows.length should be (3)
        rows(0).getStruct(0) should be (Row(1, null))
        rows(1).getStruct(0) should be (Row(null, 2))
        rows(2).isNullAt(0) should be (true)
    }

    it should "fall back to the plain schema for describe" in {
        val schema = StructType(Seq(
            StructField("name", org.apache.spark.sql.types.StringType, nullable = true,
                metadata = new org.apache.spark.sql.types.MetadataBuilder().putString("comment", "A name").build())
        ))
        val df = spark.createDataFrame(Seq(Row("Hello")).asJava, schema)

        val session = com.dimajix.flowman.execution.Session.builder().withSparkSession(spark).build()
        val described = ConnectServerPlugin.describe(session.execution, df)
        described should be (com.dimajix.flowman.types.StructType.of(df.schema))
    }
}
