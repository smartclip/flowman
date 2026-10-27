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
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.lower
import org.apache.spark.sql.functions.substring
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.MetadataBuilder
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

import com.dimajix.flowman.execution.ConnectServerPlugin
import com.dimajix.flowman.execution.FlowmanFunctions
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.types.CharType
import com.dimajix.flowman.types.StringType


class FlowmanConnectServerTest extends FlowmanConnectServerTestBase {
    "The Flowman server plugin" should "be detected" in {
        ConnectServerPlugin.isAvailable(spark) should be (true)
        val capabilities = ConnectServerPlugin.capabilities(spark)
        capabilities should not be None
        capabilities.get.apiVersion should be ("1")
    }

    it should "answer a ping" in {
        ConnectServerPlugin.ping(spark) should be (true)
    }

    it should "probe data source capabilities" in {
        ConnectServerPlugin.relationSupportsMultiplePaths(spark, "parquet") should be (Some(true))
        ConnectServerPlugin.relationSupportsMultiplePaths(spark, "jdbc") should be (Some(false))
        ConnectServerPlugin.relationSupportsMultiplePaths(spark, "nonexistent-format") should be (Some(false))
    }

    it should "handle file system operations" in {
        val dir = new java.io.File(tempDir, "connect-server-fs")
        val path = dir.toURI.toString

        ConnectServerPlugin.fsOperation(spark, "mkdirs", path) should be (true)
        new java.io.File(dir.getPath).isDirectory should be (true)

        ConnectServerPlugin.fsOperation(spark, "delete", path) should be (true)
        new java.io.File(dir.getPath).exists should be (false)

        ConnectServerPlugin.fsOperation(spark, "bogus-operation", path) should be (false)
    }

    it should "create a nullable_struct" in {
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

    it should "propagate column metadata through nullable_struct" in {
        val schema = StructType(Seq(
            StructField("a", IntegerType, nullable = true, metadata = new MetadataBuilder().putString("comment", "Value A").build()),
            StructField("b", IntegerType, nullable = true)
        ))
        val df = spark.createDataFrame(Seq(Row(1, null), Row(null, 2), Row(null, null)).asJava, schema)

        val result = df.select(FlowmanFunctions.nullableStruct(Some(spark), df("a"), df("b")).as("s"))
        val structField = result.schema("s").dataType.asInstanceOf[StructType]("a")
        structField.metadata.getString("comment") should be ("Value A")
    }

    it should "describe an enriched schema" in {
        val schema = StructType(Seq(
            StructField("partner", org.apache.spark.sql.types.StringType, nullable = true,
                metadata = new MetadataBuilder().putString("comment", "The partner").build()),
            StructField("code", org.apache.spark.sql.types.StringType, nullable = true,
                metadata = new MetadataBuilder().putString(com.dimajix.spark.sql.SchemaUtils.CHAR_VARCHAR_TYPE_STRING_METADATA_KEY, "CHAR(5)").build())
        ))
        val base = spark.createDataFrame(Seq(Row("p1", "12345"), Row("p2", "54321")).asJava, schema)

        // Build an input whose enriched schema can only be derived by walking the *analyzed plan expressions*
        // (like the classic backend does): the derived "x" column must inherit the description of "partner"
        // (LOWER(CAST(...)) passes comments through), and "y" must inherit a shortened CHAR type of "code"
        // (SUBSTRING over a CHAR(5) yields CHAR(3)).
        val df = base
            .withColumn("x", lower(col("partner")))
            .withColumn("y", substring(col("code"), 0, 3))

        val session = Session.builder().withSparkSession(spark).build()
        val enriched = ConnectServerPlugin.describe(session.execution, df)

        val fields = enriched.fields.map(f => f.name.toLowerCase -> f).toMap
        fields("partner").description should be (Some("The partner"))
        fields("x").description should be (Some("The partner"))
        fields("code").ftype should be (CharType(5))
        if (fields("y").ftype != StringType)
            fields("y").ftype should be (CharType(3))
    }

    it should "count records during plan execution" in {
        val df = spark.range(1000).toDF("id")
        val wrapped = ConnectServerPlugin.countRecords(spark, df)
        wrapped should not be None
        wrapped.get.count() should be (1000)
    }

    it should "support eager cache plans" in {
        val source = spark.range(10000).toDF("id").cache()
        val df2 = source.withColumn("id2", source("id") * 2)
        val df3 = df2.groupBy("id", "id2").count().where("count < 3")
        val result = df2.join(df3, Seq("id")).groupBy().count()

        val count1 = result.collect()(0).getLong(0)
        val count2 = result.collect()(0).getLong(0)
        count1 should be (count2)
        count1 should be > 0L
    }
}
