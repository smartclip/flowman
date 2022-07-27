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

package com.dimajix.flowman.transforms

import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.MetadataBuilder
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.execution.MigrationStrategy
import com.dimajix.flowman.execution.SchemaMismatchException
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.types.Field
import com.dimajix.flowman.types.FieldType
import com.dimajix.spark.sql.SchemaUtils
import com.dimajix.spark.testing.LocalSparkSession


class SchemaEnforcerTest extends AnyFlatSpec with Matchers with LocalSparkSession {
    "The ColumnMismatchStrategy" should "parse correctly" in {
        ColumnMismatchStrategy.ofString("IGNORE") should be (ColumnMismatchStrategy.IGNORE)
        ColumnMismatchStrategy.ofString("ignore") should be (ColumnMismatchStrategy.IGNORE)
        ColumnMismatchStrategy.ofString("ERROR") should be (ColumnMismatchStrategy.ERROR)
        ColumnMismatchStrategy.ofString("ADD_COLUMNS_OR_IGNORE") should be (ColumnMismatchStrategy.ADD_COLUMNS_OR_IGNORE)
        ColumnMismatchStrategy.ofString("ADD_COLUMNS_OR_ERROR") should be (ColumnMismatchStrategy.ADD_COLUMNS_OR_ERROR)
        ColumnMismatchStrategy.ofString("REMOVE_COLUMNS_OR_IGNORE") should be (ColumnMismatchStrategy.REMOVE_COLUMNS_OR_IGNORE)
        ColumnMismatchStrategy.ofString("REMOVE_COLUMNS_OR_ERROR") should be (ColumnMismatchStrategy.REMOVE_COLUMNS_OR_ERROR)
        ColumnMismatchStrategy.ofString("ADD_REMOVE_COLUMNS") should be (ColumnMismatchStrategy.ADD_REMOVE_COLUMNS)
        a[NullPointerException] shouldBe thrownBy(ColumnMismatchStrategy.ofString(null))
        an[IllegalArgumentException] shouldBe thrownBy(ColumnMismatchStrategy.ofString("NO_SUCH_MODE"))
    }

    it should "provide a toString method" in {
        ColumnMismatchStrategy.IGNORE.toString should be ("IGNORE")
        ColumnMismatchStrategy.ERROR.toString should be ("ERROR")
        ColumnMismatchStrategy.ADD_COLUMNS_OR_IGNORE.toString should be ("ADD_COLUMNS_OR_IGNORE")
        ColumnMismatchStrategy.ADD_COLUMNS_OR_ERROR.toString should be ("ADD_COLUMNS_OR_ERROR")
        ColumnMismatchStrategy.REMOVE_COLUMNS_OR_IGNORE.toString should be ("REMOVE_COLUMNS_OR_IGNORE")
        ColumnMismatchStrategy.REMOVE_COLUMNS_OR_ERROR.toString should be ("REMOVE_COLUMNS_OR_ERROR")
        ColumnMismatchStrategy.ADD_REMOVE_COLUMNS.toString should be ("ADD_REMOVE_COLUMNS")
    }

    it should "parse toString correctly" in {
        ColumnMismatchStrategy.ofString(ColumnMismatchStrategy.IGNORE.toString) should be (ColumnMismatchStrategy.IGNORE)
        ColumnMismatchStrategy.ofString(ColumnMismatchStrategy.ERROR.toString) should be (ColumnMismatchStrategy.ERROR)
        ColumnMismatchStrategy.ofString(ColumnMismatchStrategy.ADD_COLUMNS_OR_IGNORE.toString) should be (ColumnMismatchStrategy.ADD_COLUMNS_OR_IGNORE)
        ColumnMismatchStrategy.ofString(ColumnMismatchStrategy.ADD_COLUMNS_OR_ERROR.toString) should be (ColumnMismatchStrategy.ADD_COLUMNS_OR_ERROR)
        ColumnMismatchStrategy.ofString(ColumnMismatchStrategy.REMOVE_COLUMNS_OR_IGNORE.toString) should be (ColumnMismatchStrategy.REMOVE_COLUMNS_OR_IGNORE)
        ColumnMismatchStrategy.ofString(ColumnMismatchStrategy.REMOVE_COLUMNS_OR_ERROR.toString) should be (ColumnMismatchStrategy.REMOVE_COLUMNS_OR_ERROR)
        ColumnMismatchStrategy.ofString(ColumnMismatchStrategy.ADD_REMOVE_COLUMNS.toString) should be (ColumnMismatchStrategy.ADD_REMOVE_COLUMNS)
    }


    "The TypeMismatchStrategy" should "parse correctly" in {
        TypeMismatchStrategy.ofString("IGNORE") should be (TypeMismatchStrategy.IGNORE)
        TypeMismatchStrategy.ofString("ignore") should be (TypeMismatchStrategy.IGNORE)
        TypeMismatchStrategy.ofString("ERROR") should be (TypeMismatchStrategy.ERROR)
        TypeMismatchStrategy.ofString("CAST_COMPATIBLE_OR_ERROR") should be (TypeMismatchStrategy.CAST_COMPATIBLE_OR_ERROR)
        TypeMismatchStrategy.ofString("CAST_COMPATIBLE_OR_IGNORE") should be (TypeMismatchStrategy.CAST_COMPATIBLE_OR_IGNORE)
        TypeMismatchStrategy.ofString("CAST_ALWAYS") should be (TypeMismatchStrategy.CAST_ALWAYS)
        a[NullPointerException] shouldBe thrownBy(TypeMismatchStrategy.ofString(null))
        an[IllegalArgumentException] shouldBe thrownBy(TypeMismatchStrategy.ofString("NO_SUCH_MODE"))
    }

    it should "provide a toString method" in {
        TypeMismatchStrategy.IGNORE.toString should be ("IGNORE")
        TypeMismatchStrategy.ERROR.toString should be ("ERROR")
        TypeMismatchStrategy.CAST_COMPATIBLE_OR_ERROR.toString should be ("CAST_COMPATIBLE_OR_ERROR")
        TypeMismatchStrategy.CAST_COMPATIBLE_OR_IGNORE.toString should be ("CAST_COMPATIBLE_OR_IGNORE")
        TypeMismatchStrategy.CAST_ALWAYS.toString should be ("CAST_ALWAYS")
    }

    it should "parse toString correctly" in {
        TypeMismatchStrategy.ofString(TypeMismatchStrategy.IGNORE.toString) should be (TypeMismatchStrategy.IGNORE)
        TypeMismatchStrategy.ofString(TypeMismatchStrategy.ERROR.toString) should be (TypeMismatchStrategy.ERROR)
        TypeMismatchStrategy.ofString(TypeMismatchStrategy.CAST_COMPATIBLE_OR_ERROR.toString) should be (TypeMismatchStrategy.CAST_COMPATIBLE_OR_ERROR)
        TypeMismatchStrategy.ofString(TypeMismatchStrategy.CAST_COMPATIBLE_OR_IGNORE.toString) should be (TypeMismatchStrategy.CAST_COMPATIBLE_OR_IGNORE)
        TypeMismatchStrategy.ofString(TypeMismatchStrategy.CAST_ALWAYS.toString) should be (TypeMismatchStrategy.CAST_ALWAYS)
    }


    "A conforming schema" should "be generated for simple cases" in {
        val inputSchema = StructType(Seq(
            StructField("col1", StringType),
            StructField("COL2", IntegerType),
            StructField("col3", IntegerType)
        ))
        val requestedSchema = StructType(Seq(
            StructField("col2", StringType),
            StructField("col1", StringType),
            StructField("col4", IntegerType)
        ))

        val xfs = SchemaEnforcer(requestedSchema)
        val columns = xfs.transform(inputSchema)
        val inputDf = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], inputSchema)
        val outputDf = inputDf.select(columns:_*)
        outputDf.schema should be (StructType(Seq(
            StructField("col2", StringType),
            StructField("col1", StringType),
            StructField("col4", IntegerType)
        )))
    }

    it should "support ColumnMismatchStrategy.ADD_REMOVE_COLUMNS" in {
        val requestedSchema = StructType(Seq(
            StructField("col2", StringType),
            StructField("col1", StringType),
            StructField("col4", IntegerType)
        ))
        val xfs = SchemaEnforcer(
            requestedSchema,
            columnMismatchStrategy=ColumnMismatchStrategy.ADD_REMOVE_COLUMNS,
            typeMismatchStrategy=TypeMismatchStrategy.CAST_ALWAYS
        )

        val inputSchema = StructType(Seq(
            StructField("col1", StringType),
            StructField("COL2", IntegerType),
            StructField("col3", IntegerType)
        ))
        val inputDf = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], inputSchema)
        val outputDf = xfs.transform(inputDf)
        outputDf.schema should be (StructType(Seq(
            StructField("col2", StringType),
            StructField("col1", StringType),
            StructField("col4", IntegerType)
        )))
    }
    it should "support ColumnMismatchStrategy.IGNORE" in {
        val requestedSchema = StructType(Seq(
            StructField("col2", StringType),
            StructField("col1", StringType),
            StructField("col4", IntegerType)
        ))
        val xfs = SchemaEnforcer(
            requestedSchema,
            columnMismatchStrategy=ColumnMismatchStrategy.IGNORE,
            typeMismatchStrategy=TypeMismatchStrategy.CAST_ALWAYS
        )

        val inputSchema = StructType(Seq(
            StructField("col1", StringType),
            StructField("COL2", IntegerType),
            StructField("col3", IntegerType)
        ))
        val inputDf = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], inputSchema)
        val outputDf = xfs.transform(inputDf)
        outputDf.schema should be (StructType(Seq(
            StructField("col2", StringType),
            StructField("col1", StringType),
            StructField("col3", IntegerType)
        )))
    }
    it should "support ColumnMismatchStrategy.ERROR (1)" in {
        val requestedSchema = StructType(Seq(
            StructField("col2", StringType),
            StructField("col1", StringType),
            StructField("col4", IntegerType)
        ))
        val xfs = SchemaEnforcer(
            requestedSchema,
            columnMismatchStrategy=ColumnMismatchStrategy.ERROR,
            typeMismatchStrategy=TypeMismatchStrategy.CAST_ALWAYS
        )

        val inputSchema = StructType(Seq(
            StructField("col1", StringType),
            StructField("COL2", IntegerType),
            StructField("col4", IntegerType)
        ))
        val inputDf = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], inputSchema)
        val outputDf = xfs.transform(inputDf)
        outputDf.schema should be (StructType(Seq(
            StructField("col2", StringType),
            StructField("col1", StringType),
            StructField("col4", IntegerType)
        )))
    }
    it should "support ColumnMismatchStrategy.ERROR (2)" in {
        val requestedSchema = StructType(Seq(
            StructField("col2", StringType),
            StructField("col1", StringType),
            StructField("col4", IntegerType)
        ))
        val xfs = SchemaEnforcer(
            requestedSchema,
            columnMismatchStrategy=ColumnMismatchStrategy.ERROR,
            typeMismatchStrategy=TypeMismatchStrategy.CAST_ALWAYS
        )

        val inputSchema = StructType(Seq(
            StructField("col1", StringType),
            StructField("COL2", IntegerType),
            StructField("col3", IntegerType),
            StructField("col4", IntegerType)
        ))
        val inputDf = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], inputSchema)
        a[SchemaMismatchException] should be thrownBy(xfs.transform(inputDf))
    }
    it should "support ColumnMismatchStrategy.ERROR (3)" in {
        val requestedSchema = StructType(Seq(
            StructField("col2", StringType),
            StructField("col1", StringType),
            StructField("col4", IntegerType)
        ))
        val xfs = SchemaEnforcer(
            requestedSchema,
            columnMismatchStrategy=ColumnMismatchStrategy.ERROR,
            typeMismatchStrategy=TypeMismatchStrategy.CAST_ALWAYS
        )

        val inputSchema = StructType(Seq(
            StructField("col1", StringType),
            StructField("COL2", IntegerType)
        ))
        val inputDf = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], inputSchema)
        a[SchemaMismatchException] should be thrownBy(xfs.transform(inputDf))
    }

    it should "support ColumnMismatchStrategy.ADD_COLUMNS_OR_IGNORE (1)" in {
        val requestedSchema = StructType(Seq(
            StructField("col2", StringType),
            StructField("col1", StringType),
            StructField("col3", StringType),
            StructField("col4", IntegerType)
        ))
        val xfs = SchemaEnforcer(
            requestedSchema,
            columnMismatchStrategy=ColumnMismatchStrategy.ADD_COLUMNS_OR_IGNORE,
            typeMismatchStrategy=TypeMismatchStrategy.CAST_ALWAYS
        )

        val inputSchema = StructType(Seq(
            StructField("col1", StringType),
            StructField("COL2", IntegerType),
            StructField("col4", IntegerType)
        ))
        val inputDf = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], inputSchema)
        val outputDf = xfs.transform(inputDf)
        outputDf.schema should be (StructType(Seq(
            StructField("col2", StringType),
            StructField("col1", StringType),
            StructField("col3", StringType),
            StructField("col4", IntegerType)
        )))
    }
    it should "support ColumnMismatchStrategy.ADD_COLUMNS_OR_IGNORE (2)" in {
        val requestedSchema = StructType(Seq(
            StructField("col2", StringType),
            StructField("col1", StringType),
            StructField("col4", IntegerType)
        ))
        val xfs = SchemaEnforcer(
            requestedSchema,
            columnMismatchStrategy=ColumnMismatchStrategy.ADD_COLUMNS_OR_IGNORE,
            typeMismatchStrategy=TypeMismatchStrategy.CAST_ALWAYS
        )

        val inputSchema = StructType(Seq(
            StructField("col1", StringType),
            StructField("COL2", IntegerType),
            StructField("col3", IntegerType)
        ))
        val inputDf = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], inputSchema)
        val outputDf = xfs.transform(inputDf)
        outputDf.schema should be (StructType(Seq(
            StructField("col2", StringType),
            StructField("col1", StringType),
            StructField("col4", IntegerType),
            StructField("col3", IntegerType)
        )))
    }

    it should "support ColumnMismatchStrategy.ADD_COLUMNS_OR_ERROR (1)" in {
        val requestedSchema = StructType(Seq(
            StructField("col2", StringType),
            StructField("col1", StringType),
            StructField("col3", StringType),
            StructField("col4", IntegerType)
        ))
        val xfs = SchemaEnforcer(
            requestedSchema,
            columnMismatchStrategy=ColumnMismatchStrategy.ADD_COLUMNS_OR_ERROR,
            typeMismatchStrategy=TypeMismatchStrategy.CAST_ALWAYS
        )

        val inputSchema = StructType(Seq(
            StructField("col1", StringType),
            StructField("COL2", IntegerType),
            StructField("col4", IntegerType)
        ))
        val inputDf = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], inputSchema)
        val outputDf = xfs.transform(inputDf)
        outputDf.schema should be (StructType(Seq(
            StructField("col2", StringType),
            StructField("col1", StringType),
            StructField("col3", StringType),
            StructField("col4", IntegerType)
        )))
    }
    it should "support ColumnMismatchStrategy.ADD_COLUMNS_OR_ERROR (2)" in {
        val requestedSchema = StructType(Seq(
            StructField("col2", StringType),
            StructField("col1", StringType),
            StructField("col4", IntegerType)
        ))
        val xfs = SchemaEnforcer(
            requestedSchema,
            columnMismatchStrategy=ColumnMismatchStrategy.ADD_COLUMNS_OR_ERROR,
            typeMismatchStrategy=TypeMismatchStrategy.CAST_ALWAYS
        )

        val inputSchema = StructType(Seq(
            StructField("col1", StringType),
            StructField("COL2", IntegerType),
            StructField("col3", IntegerType)
        ))
        val inputDf = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], inputSchema)
        a[SchemaMismatchException] should be thrownBy(xfs.transform(inputDf))
    }

    it should "support ColumnMismatchStrategy.REMOVE_COLUMNS_OR_IGNORE (1)" in {
        val requestedSchema = StructType(Seq(
            StructField("col2", StringType),
            StructField("col1", StringType),
            StructField("col3", StringType),
            StructField("col4", IntegerType)
        ))
        val xfs = SchemaEnforcer(
            requestedSchema,
            columnMismatchStrategy=ColumnMismatchStrategy.REMOVE_COLUMNS_OR_IGNORE,
            typeMismatchStrategy=TypeMismatchStrategy.CAST_ALWAYS
        )

        val inputSchema = StructType(Seq(
            StructField("col1", StringType),
            StructField("COL2", IntegerType),
            StructField("col4", IntegerType)
        ))
        val inputDf = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], inputSchema)
        val outputDf = xfs.transform(inputDf)
        outputDf.schema should be (StructType(Seq(
            StructField("col2", StringType),
            StructField("col1", StringType),
            StructField("col4", IntegerType)
        )))
    }
    it should "support ColumnMismatchStrategy.REMOVE_COLUMNS_OR_IGNORE (2)" in {
        val requestedSchema = StructType(Seq(
            StructField("col2", StringType),
            StructField("col1", StringType),
            StructField("col3", StringType)
        ))
        val xfs = SchemaEnforcer(
            requestedSchema,
            columnMismatchStrategy=ColumnMismatchStrategy.REMOVE_COLUMNS_OR_IGNORE,
            typeMismatchStrategy=TypeMismatchStrategy.CAST_ALWAYS
        )

        val inputSchema = StructType(Seq(
            StructField("col1", StringType),
            StructField("COL2", IntegerType),
            StructField("col3", IntegerType),
            StructField("col4", IntegerType)
        ))
        val inputDf = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], inputSchema)
        val outputDf = xfs.transform(inputDf)
        outputDf.schema should be (StructType(Seq(
            StructField("col2", StringType),
            StructField("col1", StringType),
            StructField("col3", StringType)
        )))
    }

    it should "support ColumnMismatchStrategy.REMOVE_COLUMNS_OR_ERROR (1)" in {
        val requestedSchema = StructType(Seq(
            StructField("col2", StringType),
            StructField("col1", StringType),
            StructField("col3", StringType)
        ))
        val xfs = SchemaEnforcer(
            requestedSchema,
            columnMismatchStrategy=ColumnMismatchStrategy.REMOVE_COLUMNS_OR_ERROR,
            typeMismatchStrategy=TypeMismatchStrategy.CAST_ALWAYS
        )

        val inputSchema = StructType(Seq(
            StructField("col1", StringType),
            StructField("COL2", IntegerType),
            StructField("col3", StringType),
            StructField("col4", IntegerType)
        ))
        val inputDf = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], inputSchema)
        val outputDf = xfs.transform(inputDf)
        outputDf.schema should be (StructType(Seq(
            StructField("col2", StringType),
            StructField("col1", StringType),
            StructField("col3", StringType)
        )))
    }
    it should "support ColumnMismatchStrategy.REMOVE_COLUMNS_OR_ERROR (2)" in {
        val requestedSchema = StructType(Seq(
            StructField("col2", StringType),
            StructField("col1", StringType),
            StructField("col4", IntegerType)
        ))
        val xfs = SchemaEnforcer(
            requestedSchema,
            columnMismatchStrategy=ColumnMismatchStrategy.REMOVE_COLUMNS_OR_ERROR,
            typeMismatchStrategy=TypeMismatchStrategy.CAST_ALWAYS
        )

        val inputSchema = StructType(Seq(
            StructField("col1", StringType),
            StructField("COL2", IntegerType),
            StructField("col3", IntegerType)
        ))
        val inputDf = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], inputSchema)
        a[SchemaMismatchException] should be thrownBy(xfs.transform(inputDf))
    }

    it should "work with nested entities" in {
        val inputSchema = StructType(Seq(
            StructField("col1", StringType),
            StructField("COL2", StructType(
                Seq(
                    StructField("nested1", StringType),
                    StructField("nested3", FloatType),
                    StructField("nested4", StructType(
                        Seq(
                            StructField("nested4_1", StringType),
                            StructField("nested4_2", FloatType)
                        )
                    )),
                    StructField("nested5", StructType(
                        Seq(
                            StructField("nested5_1", StringType),
                            StructField("nested5_2", FloatType)
                        )
                    ))
                )
            )),
            StructField("col3", IntegerType)
        ))
        val requestedSchema = StructType(Seq(
            StructField("col2", StructType(
                Seq(
                    StructField("nested1", LongType),
                    StructField("nested2", FloatType),
                    StructField("nested4", StructType(
                        Seq(
                            StructField("nested4_1", StringType),
                            StructField("nested4_3", FloatType)
                        )
                    )),
                    StructField("nested5", StructType(
                        Seq(
                            StructField("nested5_1", StringType),
                            StructField("nested5_2", FloatType)
                        )
                    ))
                )
            )),
            StructField("col1", StringType),
            StructField("col4", IntegerType)
        ))

        val xfs = SchemaEnforcer(requestedSchema)
        val columns = xfs.transform(inputSchema)
        val inputDf = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], inputSchema)
        val outputDf = inputDf.select(columns:_*)
        outputDf.schema should be (StructType(Seq(
            StructField("col2", StructType(
                Seq(
                    StructField("nested1", LongType),
                    StructField("nested2", FloatType),
                    StructField("nested4", StructType(
                        Seq(
                            StructField("nested4_1", StringType),
                            StructField("nested4_3", FloatType)
                        )
                    )),
                    StructField("nested5", StructType(
                        Seq(
                            StructField("nested5_1", StringType),
                            StructField("nested5_2", FloatType)
                        )
                    ))
                )
            )),
            StructField("col1", StringType),
            StructField("col4", IntegerType)
        )))
    }

    "The SchemaEnforcer" should "support extended string attributes" in {
        val inputDf = spark.createDataFrame(Seq(
            ("col1", "12"),
            ("col2", "23")
        ))
            .withColumn("_1", col("_1").as("_1"))
            .withColumn("_2", col("_2").as("_2"))
            .withColumn("_5", col("_2").as("_2"))
        val inputSchema = inputDf.schema

        val requestedSchema = com.dimajix.flowman.types.StructType(Seq(
            Field("_1", FieldType.of("varchar(10)")),
            Field("_2", FieldType.of("char(20)")),
            Field("_3", FieldType.of("string"))
        ))
        val xfs = SchemaEnforcer(requestedSchema.sparkType)

        val columns = xfs.transform(inputSchema)
        val outputDf = inputDf.select(columns:_*)
        SchemaUtils.dropMetadata(outputDf.schema) should be (StructType(Seq(
            StructField("_1", StringType),
            StructField("_2", StringType),
            StructField("_3", StringType)
        )))
        com.dimajix.flowman.types.StructType.of(outputDf.schema) should be (com.dimajix.flowman.types.StructType(Seq(
            Field("_1", FieldType.of("varchar(10)")),
            Field("_2", FieldType.of("char(20)")),
            Field("_3", FieldType.of("string"))
        )))
    }

    it should "support comments" in {
        val inputDf = spark.createDataFrame(Seq(
            ("col1", "12"),
            ("col2", "23")
        ))
            .withColumn("_1", col("_1").as("_1", new MetadataBuilder().putString("comment","This is _1 original").build()))
            .withColumn("_2", col("_2").as("_2", new MetadataBuilder().putString("comment","This is _2 original").build()))
            .withColumn("_5", col("_2").as("_2"))
        val inputSchema = inputDf.schema

        val requestedSchema = com.dimajix.flowman.types.StructType(Seq(
            Field("_1", FieldType.of("int"), description=None),
            Field("_2", FieldType.of("int"), description=Some("This is _2")),
            Field("_3", FieldType.of("int"), description=Some("This is _3"))
        ))
        val xfs = SchemaEnforcer(requestedSchema.catalogType)

        val columns = xfs.transform(inputSchema)
        val outputDf = inputDf.select(columns:_*)
        outputDf.schema should be (com.dimajix.flowman.types.StructType(Seq(
            Field("_1", FieldType.of("int"), description=Some("This is _1 original")),
            Field("_2", FieldType.of("int"), description=Some("This is _2")),
            Field("_3", FieldType.of("int"), description=Some("This is _3"))
        )).catalogType)
        com.dimajix.flowman.types.StructType.of(outputDf.schema) should be (com.dimajix.flowman.types.StructType(Seq(
            Field("_1", FieldType.of("int"), description=Some("This is _1 original")),
            Field("_2", FieldType.of("int"), description=Some("This is _2")),
            Field("_3", FieldType.of("int"), description=Some("This is _3"))
        )))
    }

    it should "support collations" in {
        val inputDf = spark.createDataFrame(Seq(
            ("col1", "12", "33")
        ))
            .withColumn("_1", col("_1").as("_1"))
            .withColumn("_2", col("_2").as("_2", new MetadataBuilder().putString("collation","_2_orig").build()))
            .withColumn("_3", col("_3").as("_3", new MetadataBuilder().putString("collation","_3_orig").build()))
            .withColumn("_5", col("_3").as("_3"))
        val inputSchema = inputDf.schema

        val requestedSchema = com.dimajix.flowman.types.StructType(Seq(
            Field("_1", FieldType.of("string"), collation=None),
            Field("_2", FieldType.of("string"), collation=Some("_2")),
            Field("_3", FieldType.of("string"), collation=None),
            Field("_4", FieldType.of("string"), collation=Some("_4"))
        ))

        val xfs = SchemaEnforcer(requestedSchema.catalogType)
        val columns = xfs.transform(inputSchema)
        val outputDf = inputDf.select(columns:_*)
        outputDf.schema should be (StructType(Seq(
            StructField("_1", StringType),
            StructField("_2", StringType, metadata=new MetadataBuilder().putString("collation","_2").build()),
            StructField("_3", StringType),
            StructField("_4", StringType, metadata=new MetadataBuilder().putString("collation","_4").build()))
        ))
        com.dimajix.flowman.types.StructType.of(outputDf.schema) should be (com.dimajix.flowman.types.StructType(Seq(
            Field("_1", FieldType.of("string"), collation=None),
            Field("_2", FieldType.of("string"), collation=Some("_2")),
            Field("_3", FieldType.of("string"), collation=None),
            Field("_4", FieldType.of("string"), collation=Some("_4"))
        )))
    }
}
