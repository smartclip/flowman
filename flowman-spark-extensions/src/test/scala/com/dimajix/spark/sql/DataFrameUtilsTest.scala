/*
 * Copyright 2021 Kaya Kupferschmidt
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

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.View
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.DecimalType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.storage.StorageLevel
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.spark.testing.LocalSparkSession


class DataFrameUtilsTest extends AnyFlatSpec with Matchers with LocalSparkSession {
    implicit class ExtractPlan(opt:Option[LogicalPlan]) {
        def plan : Option[LogicalPlan] = {
            opt.map {
                // Spark >= 3.2
                case view:View => view.child
                // Spark < 3.2
                case df:LogicalPlan => df
            }
        }
    }

    "DataFrameUtils.compare" should "work" in {
        val schema = StructType(Seq(
            StructField("c1", IntegerType),
            StructField("c2", StringType),
            StructField("c3", DoubleType),
            StructField("c4", DecimalType(30,6)),
            StructField("c5", DateType)
        ))
        val lines = Seq(
            Array("1","lala","2.3","2.5","2019-02-01"),
            Array("2","","3.4","",""),
            Array("",null,"",null,null)
        )
        val df1 = DataFrameBuilder.ofStringValues(spark, lines, schema)
        val df2 = DataFrameBuilder.ofStringValues(spark, lines, schema)

        DataFrameUtils.compare(df1, df2) should be (true)
        DataFrameUtils.compare(df1.limit(2), df2) should be (false)
        DataFrameUtils.compare(df1, df2.limit(2)) should be (false)
        DataFrameUtils.compare(df1.drop("c1"), df2) should be (false)
        DataFrameUtils.compare(df1, df2.drop("c1")) should be (false)
    }

    "DataFrameUtils.diff" should "work" in {
        val schema = StructType(Seq(
            StructField("c1", IntegerType),
            StructField("c2", StringType),
            StructField("c3", DoubleType),
            StructField("c4", DecimalType(30,6)),
            StructField("c5", DateType)
        ))
        val lines = Seq(
            Array("1","lala","2.3","4.5","2019-02-01"),
            Array("2","","3.4","", ""),
            Array("",null,"",null, null)
        )
        val df1 = DataFrameBuilder.ofStringValues(spark, lines, schema)
        val df2 = DataFrameBuilder.ofStringValues(spark, lines, schema)

        DataFrameUtils.diff(df1, df2) should be (None)
        DataFrameUtils.diff(df1.limit(2), df2) should not be (None)
        DataFrameUtils.diff(df1, df2.limit(2)) should not be (None)
    }

    "DataFrameUtils.diffToStringValues" should "work" in {
        val schema = StructType(Seq(
            StructField("c1", IntegerType),
            StructField("c2", StringType),
            StructField("c3", DoubleType),
            StructField("c4", DecimalType(30,6)),
            StructField("c5", DateType),
            StructField("c6", TimestampType)
        ))
        val lines = Seq(
            Array("1","lala","2.3","2.5","2019-02-01", "2021-01-01T00:00:00+00:00"),
            Array("2","","3.4","","",""),
            Array("",null,"",null,null,null)
        )
        val df1 = DataFrameBuilder.ofStringValues(spark, lines, schema)

        DataFrameUtils.diffToStringValues(lines, df1) should be (None)
        DataFrameUtils.diffToStringValues(lines, df1.limit(2)) should not be (None)
        DataFrameUtils.diffToStringValues(lines.take(2), df1) should not be (None)
    }

    "DataFrameUtils.withCaches" should "cache und uncache DataFrames" in {
        val df = spark.emptyDataFrame

        df.storageLevel should be (StorageLevel.NONE)

        DataFrameUtils.withCaches(Seq(df)) {
            df.storageLevel should be (StorageLevel.MEMORY_AND_DISK)
        }

        df.storageLevel should be (StorageLevel.NONE)
    }

    it should "cache und uncache in presence of exceptions" in {
        val df = spark.emptyDataFrame

        df.storageLevel should be (StorageLevel.NONE)

        an[IllegalArgumentException] should be thrownBy(
            DataFrameUtils.withCaches(Seq(df)) {
                df.storageLevel should be (StorageLevel.MEMORY_AND_DISK)
                throw new IllegalArgumentException()
            })

        df.storageLevel should be (StorageLevel.NONE)
    }

    "DataFrameUtils.withTempViews" should "create and unregister temp views" in {
        val df = spark.emptyDataFrame

        spark.sessionState.catalog.getTempView("temp") should be (None)

        DataFrameUtils.withTempViews(Seq("temp" -> df)) {
            spark.sessionState.catalog.getTempView("temp").plan should be (Some(df.queryExecution.logical))
        }

        spark.sessionState.catalog.getTempView("temp") should be (None)
    }

    it should "create and unregister  temp views in presence of exceptions" in {
        val df = spark.emptyDataFrame

        spark.sessionState.catalog.getTempView("temp") should be (None)

        an[IllegalArgumentException] should be thrownBy(
            DataFrameUtils.withTempViews(Seq("temp" -> df)) {
                spark.sessionState.catalog.getTempView("temp").plan should be (Some(df.queryExecution.logical))
                throw new IllegalArgumentException()
            })

        spark.sessionState.catalog.getTempView("temp") should be (None)
    }
}
