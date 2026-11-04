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

import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkShim
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.ExpressionInfo
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.struct
import org.apache.spark.sql.functions.when
import org.apache.spark.util.LongAccumulator
import com.dimajix.spark.sql.catalyst.plans.logical.CountRecords
import com.dimajix.spark.sql.expressions.CreateNullableStruct


class functions
object functions {
    private val connectMode = new java.util.concurrent.atomic.AtomicBoolean(false)

    /**
      * Tells this object whether expressions will be evaluated against a Spark Connect session. This is maintained
      * by [[com.dimajix.flowman.execution.SparkBackend]], since some helper functions need to avoid classic-only
      * Catalyst expressions when running against a Spark Connect endpoint.
      */
    def setConnectMode(connect:Boolean) : Unit = connectMode.set(connect)
    def isConnectMode : Boolean = connectMode.get()

    /**
      * Creates a struct with the given values which evaluates to `NULL` if and only if all values are `NULL`.
      * In classic Spark mode this uses the `named_nullable_struct` expression, which also preserves metadata of
      * aliased child expressions. In Spark Connect mode the expression cannot be serialized by the client, so a
      * semantically equivalent variant built from the public API is used instead (at the cost of dropping the
      * child metadata and possibly evaluating non-deterministic children twice).
      */
    @scala.annotation.varargs
    def nullable_struct(cols: Column*): Column = {
        if (!isConnectMode)
            SparkShim.column(CreateNullableStruct(cols.map(SparkShim.expr)))
        else if (cols.isEmpty)
            when(lit(true), lit(null)).otherwise(struct())
        else
            when(cols.map(_.isNull).reduce(_ && _), lit(null)).otherwise(struct(cols:_*))
    }

    def count_records(df:DataFrame, counter:LongAccumulator) : DataFrame =
        DataFrameBuilder.ofRows(df.sparkSession, CountRecords(df.queryExecution.logical, counter))


    type FunctionDescription = (FunctionIdentifier, ExpressionInfo, FunctionBuilder)
    def wrap(fn:(Column) => Column, name:String) : FunctionDescription = {
        (
            FunctionIdentifier(name),
            new ExpressionInfo(classOf[functions].getCanonicalName, name),
            exprs => SparkShim.expr(fn(SparkShim.column(exprs(0))))
        )
    }
    def wrap(fn:(Column, Column) => Column, name:String) : FunctionDescription = {
        (
            FunctionIdentifier(name),
            new ExpressionInfo(classOf[functions].getCanonicalName, name),
            exprs => SparkShim.expr(fn(SparkShim.column(exprs(0)), SparkShim.column(exprs(1))))
        )
    }
    def wrap(fn:(Column, Column, Column) => Column, name:String) : FunctionDescription = {
        (
            FunctionIdentifier(name),
            new ExpressionInfo(classOf[functions].getCanonicalName, name),
            exprs => SparkShim.expr(fn(SparkShim.column(exprs(0)), SparkShim.column(exprs(1)), SparkShim.column(exprs(2))))
        )
    }
    def wrap(fn:(Column, Column, Column, Column) => Column, name:String) : FunctionDescription = {
        (
            FunctionIdentifier(name),
            new ExpressionInfo(classOf[functions].getCanonicalName, name),
            exprs => SparkShim.expr(fn(SparkShim.column(exprs(0)), SparkShim.column(exprs(1)), SparkShim.column(exprs(2)), SparkShim.column(exprs(3))))
        )
    }

    def register(spark:SparkSession, fn:FunctionDescription) : Unit = {
        val (id, info, builder) = fn
        SparkShim.registerFunction(spark, id, info, builder)
    }
    def register(spark:SparkSession, fn:(Column) => Column, name:String) : Unit = {
        register(spark, wrap(fn, name))
    }
    def register(spark:SparkSession, fn:(Column, Column) => Column, name:String) : Unit = {
        register(spark, wrap(fn, name))
    }
    def register(spark:SparkSession, fn:(Column, Column, Column) => Column, name:String) : Unit = {
        register(spark, wrap(fn, name))
    }
    def register(spark:SparkSession, fn:(Column, Column, Column, Column) => Column, name:String) : Unit = {
        register(spark, wrap(fn, name))
    }
}
