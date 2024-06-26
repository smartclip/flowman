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

import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.model.Mapping
import com.dimajix.flowman.model.MappingIdentifier
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.model.Module
import com.dimajix.flowman.transforms.CaseFormat
import com.dimajix.flowman.{types => ftypes}
import com.dimajix.spark.testing.LocalSparkSession


class FlattenMappingTest extends AnyFlatSpec with Matchers with LocalSparkSession{
    "A FlattenMapping" should "be parseable" in {
        val spec =
            """
              |mappings:
              |  my_structure:
              |    kind: flatten
              |    naming: camelCase
            """.stripMargin

        val project = Module.read.string(spec).toProject("project")
        val mapping = project.mappings("my_structure")
        mapping shouldBe an[FlattenMappingSpec]

        val session = Session.builder().disableSpark().build()
        val context = session.getContext(project)
        val instance = context.getMapping(MappingIdentifier("my_structure"))
        instance shouldBe an[FlattenMapping]
    }

    it should "flatten nested structures" in {
        val inputJson =
            """
              |{
              |  "stupid_name": {
              |    "secret_struct": {
              |      "secret_field":123,
              |      "other_field":456
              |    }
              |  }
              |}""".stripMargin

        val spark = this.spark
        import spark.implicits._

        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.execution

        val inputRecords = Seq(inputJson.replace("\n",""))
        val inputDs = spark.createDataset(inputRecords)
        val inputDf = spark.read.json(inputDs)

        val mapping = FlattenMapping(
            Mapping.Properties(session.context),
            MappingOutputIdentifier("input_df"),
            CaseFormat.SNAKE_CASE
        )

        val expectedSchema = StructType(Seq(
            StructField("stupid_name_secret_struct_other_field", LongType),
            StructField("stupid_name_secret_struct_secret_field", LongType)
        ))

        val outputDf = mapping.execute(executor, Map(MappingOutputIdentifier("input_df") -> inputDf))("main")
        outputDf.schema should be (expectedSchema)

        val outputSchema = mapping.describe(executor, Map(MappingOutputIdentifier("input_df") -> ftypes.StructType.of(inputDf.schema)))("main")
        outputSchema.sparkType should be (expectedSchema)
    }
}
