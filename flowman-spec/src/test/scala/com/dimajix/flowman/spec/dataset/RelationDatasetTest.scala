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

package com.dimajix.flowman.spec.dataset

import org.apache.hadoop.fs.Path
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.common.Yes
import com.dimajix.flowman.execution.OutputMode
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.model.Project
import com.dimajix.flowman.model.Relation
import com.dimajix.flowman.model.RelationIdentifier
import com.dimajix.flowman.model.ResourceIdentifier
import com.dimajix.flowman.model.Prototype
import com.dimajix.flowman.spec.ObjectMapper
import com.dimajix.flowman.types.SingleValue
import com.dimajix.flowman.types.StructType
import com.dimajix.spark.testing.LocalSparkSession


object RelationDatasetTest {
}

class RelationDatasetTest extends AnyFlatSpec with Matchers with MockFactory with LocalSparkSession {
    "The RelationDataset" should "be parsable" in {
        val spec =
            """
              |kind: relation
              |relation: some_relation
              |""".stripMargin
        val ds = ObjectMapper.parse[DatasetSpec](spec)
        ds shouldBe a[RelationDatasetSpec]
    }

    it should "support embedded relations" in {
        val spec =
            """
              |kind: relation
              |relation:
              |  kind: empty
              |""".stripMargin
        val ds = ObjectMapper.parse[DatasetSpec](spec)
        ds shouldBe a[RelationDatasetSpec]
    }

    it should "work" in {
        val relation = mock[Relation]
        val relationSpec = mock[Prototype[Relation]]
        (relationSpec.instantiate _).expects(*, None).returns(relation)

        val project = Project(
            name="test",
            relations = Map("relation" -> relationSpec)
        )

        val session = Session.builder.withSparkSession(spark).build()
        val context = session.getContext(project)
        val executor = session.execution

        val dataset = RelationDataset(
            context,
            RelationIdentifier("relation"),
            Map[String,SingleValue]()
        )

        (relation.provides _).expects(*,*).returns(Set(ResourceIdentifier.ofFile(new Path("provided_file"))))
        dataset.provides should be (Set(
            ResourceIdentifier.ofFile(new Path("provided_file"))
        ))

        (relation.requires _).expects(*,*).returns(Set(ResourceIdentifier.ofFile(new Path("required_file"))))
        dataset.requires should be (Set(
            ResourceIdentifier.ofFile(new Path("required_file"))
        ))

        (relation.loaded _).expects(executor,*).returns(Yes)
        dataset.exists(executor) should be (Yes)

        (relation.truncate _).expects(executor,*).returns(Unit)
        dataset.clean(executor)

        (relation.read _).expects(executor,*).returns(null)
        dataset.read(executor)

        (relation.write _).expects(executor,spark.emptyDataFrame,*,OutputMode.APPEND).returns(Unit)
        dataset.write(executor, spark.emptyDataFrame, OutputMode.APPEND)

        (relation.identifier _).expects().returns(RelationIdentifier("relation"))
        (relation.describe _).expects(executor, *).returns(new StructType())
        dataset.describe(executor) should be (Some(new StructType()))
    }
}
