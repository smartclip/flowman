/*
 * Copyright 2018-2021 Kaya Kupferschmidt
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

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame
import org.scalatest.FlatSpec
import org.scalatest.Matchers

import scala.collection.mutable

import com.dimajix.flowman.execution.MappingGraphTest.DummyMappingSpec
import com.dimajix.flowman.model.BaseMapping
import com.dimajix.flowman.model.Mapping
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.model.Project
import com.dimajix.flowman.model.Template


object MappingGraphTest {
    case class DummyMapping(
        override val context: Context,
        override val name: String,
        override val inputs: Seq[MappingOutputIdentifier]
    ) extends BaseMapping {
        protected override def instanceProperties: Mapping.Properties = Mapping.Properties(context, name)
        override def execute(execution: Execution, input: Map[MappingOutputIdentifier, DataFrame]): Map[String, DataFrame] = ???
    }

    case class DummyMappingSpec(
        name: String,
        inputs: Seq[MappingOutputIdentifier]
    ) extends Template[Mapping] {
        override def instantiate(context: Context): Mapping = DummyMapping(context, name, inputs)
    }
}

class MappingGraphTest extends FlatSpec with Matchers {
    def depthFirstTraversal( node: Node ): Seq[String] = {
        if ( node.children.isEmpty ) {
            Seq( node.mappingOutputIdentifier.name )
        } else {
            val children = node
                .children
                .toSeq
                .flatMap( child => depthFirstTraversal( child ) )

            node.mappingOutputIdentifier.name +: children
        }
    }

    "The MappingUtils" should "detect cycles in branches" in {
        val project = Project(
            "test",
            mappings = Map(
                "c3" -> DummyMappingSpec(
                    "c3",
                    Seq( MappingOutputIdentifier("c1") )
                ),
                "c2" -> DummyMappingSpec(
                    "c2",
                    Seq( MappingOutputIdentifier("c3") )
                ),
                "c1" -> DummyMappingSpec(
                    "c1",
                    Seq( MappingOutputIdentifier("c2") )
                )
            )
        )

        val session = Session.builder.build
        val context = session.getContext( project )

        an[IllegalArgumentException] should be thrownBy (
            new MappingGraph().getNode( MappingOutputIdentifier("c1"), context )
        )
    }

    it should "detect merging branches" in {
        val project = Project(
            "test",
            mappings = Map(
                "mb1" -> DummyMappingSpec(
                    "mb1",
                    Seq( MappingOutputIdentifier("mb2"), MappingOutputIdentifier("mb3") )
                ),
                "mb2" -> DummyMappingSpec(
                    "mb2",
                    Seq( MappingOutputIdentifier("mb4") )
                ),
                "mb3" -> DummyMappingSpec(
                    "mb3",
                    Seq( MappingOutputIdentifier("mb4") )
                ),
                "mb4" -> DummyMappingSpec(
                    "mb4",
                    Seq (MappingOutputIdentifier("mb5") )
                ),
                "mb5" -> DummyMappingSpec(
                    "mb5",
                    Seq()
                )
            )
        )

        val session = Session.builder.build
        val context = session.getContext( project )

        depthFirstTraversal(
            new MappingGraph().getNode( MappingOutputIdentifier("mb1"), context )
        )should be (
            Seq("mb1", "mb2", "mb4", "mb5", "mb3", "mb4", "mb5")
        )
    }
}
