/*
 * Copyright (C) 2021 The Flowman Authors
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

package com.dimajix.spark.sql


import scala.collection.JavaConverters._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.classic.{SparkSession => ClassicSparkSession,DataFrameBridge}
import org.apache.spark.sql.SparkShim.expressionEncoderFor
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.types.StructType
import com.dimajix.spark.sql.catalyst.PlanUtils


object DataFrameBuilder {
    private val rowParserOptions = RowParser.Options()

    /**
     * Creates a DataFrame from a sequence of string array records. The string values will be converted to appropriate
     * data types as specified in the schema
     *
     * @param sparkSession
     * @param lines
     * @param schema
     * @return
     */
    def ofStringValues(sparkSession: SparkSession, lines:Seq[Array[String]], schema:StructType) : DataFrame = {
        val reader = new RowParser(schema, rowParserOptions)
        val rows = lines.map(reader.parse)
        sparkSession.createDataFrame(rows.asJava, schema)
    }

    /**
     * Create an empty [[DataFrame]] from a schema
     *
     * @param sparkSession
     * @param schema
     * @return
     */
    def ofSchema(sparkSession: SparkSession, schema:StructType) : DataFrame = {
        ofRows(sparkSession, Seq.empty[Row], schema)
    }

    /**
     * Creates a DataFrame from a sequence of Spark [[Row]] objects and a Spark schema.
     *
     * @param sparkSession
     * @param rows
     * @param schema
     * @return
     */
    def ofRows(sparkSession: SparkSession, rows:Seq[Row], schema:StructType): DataFrame = {
        if (schema.isEmpty) {
            // SparkSession.createDataFrame(rdd, empty schema) silently ignores the row payload, whereas the
            // Seq-based overload converts through LocalRelation.fromExternalRows and throws an
            // ArrayIndexOutOfBoundsException for rows with more fields. Reproduce the RDD behavior here.
            sparkSession.createDataFrame(rows.map(_ => Row.empty).asJava, schema)
        }
        else {
            sparkSession.createDataFrame(rows.asJava, schema)
        }
    }

    /**
     * Creates a DataFrame from a Spark [[LogicalPlan]]
     *
     * @param sparkSession
     * @param logicalPlan
     * @return
     */
    def ofRows(sparkSession: SparkSession, logicalPlan: LogicalPlan): DataFrame = {
        val qe = sparkSession.sessionState.executePlan(logicalPlan)
        qe.assertAnalyzed()
        DataFrameBridge.ofRows(sparkSession.asInstanceOf[ClassicSparkSession], logicalPlan, () => expressionEncoderFor(qe.analyzed.schema))
    }

    /**
     * Creates a DataFrame containing a single Row for a given Schema, either with NULL values or with default values.
     *
     * @param sparkSession
     * @param schema
     * @return
     */
    def singleRow(sparkSession: SparkSession, schema: StructType): DataFrame = {
        if (sparkSession.getClass.getName == "org.apache.spark.sql.connect.SparkSession") {
            // Build a single Row with NULL values or default values. The classic path constructs a logical plan
            // which is not supported in Spark Connect, therefore a plain createDataFrame is used instead.
            val values = schema.fields.map { field =>
                if (field.nullable)
                    null
                else
                    CatalystTypeConverters.convertToScala(Literal.default(field.dataType).value, field.dataType)
            }
            sparkSession.createDataFrame(Seq(Row.fromSeq(values)).asJava, schema)
        }
        else {
            val logicalPlan = PlanUtils.singleRowPlan(schema)
            DataFrameBridge.ofRows(sparkSession.asInstanceOf[ClassicSparkSession], logicalPlan)
        }
    }

    /**
     * Creates a DataFrame containing a single Row for a given Schema, either with NULL values or with default values.
     *
     * @param sparkSession
     * @param schema
     * @return
     */
    def namedAttributes(sparkSession: SparkSession, schema: StructType): DataFrame = {
        val logicalPlan = PlanUtils.namedAttributePlan(schema)
        DataFrameBridge.ofRows(sparkSession.asInstanceOf[ClassicSparkSession], logicalPlan)
    }
}
