/*
 * Copyright 2022 Kaya Kupferschmidt
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

package com.dimajix.flowman.spec.documentation

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.documentation.NotNullColumnCheck
import com.dimajix.flowman.execution.RootContext
import com.dimajix.flowman.spec.ObjectMapper


class RelationDocTest extends AnyFlatSpec with Matchers {
    "A RelationDocSpec" should "be deserializable" in {
        val yaml =
            """
              |description: "This is a mapping"
              |columns:
              |  - name: col_a
              |    description: "This is column a"
              |    checks:
              |      - kind: notNull
              |  - name: col_x
              |    description: "Column of other output"
              |    columns:
              |      - name: sub_col
              |""".stripMargin

        val spec = ObjectMapper.parse[RelationDocSpec](yaml)

        val context = RootContext.builder().build()
        val relation = spec.instantiate(context)

        relation.description should be (Some("This is a mapping"))

        val mainSchema = relation.schema.get
        mainSchema.columns.size should be (2)
        mainSchema.columns(0).name should be ("col_a")
        mainSchema.columns(0).description should be (Some("This is column a"))
        mainSchema.columns(0).checks.size should be (1)
        mainSchema.columns(0).checks(0) shouldBe a[NotNullColumnCheck]
        mainSchema.columns(1).name should be ("col_x")
        mainSchema.columns(1).description should be (Some("Column of other output"))
        mainSchema.columns(1).checks.size should be (0)
        mainSchema.checks.size should be (0)
    }
}
