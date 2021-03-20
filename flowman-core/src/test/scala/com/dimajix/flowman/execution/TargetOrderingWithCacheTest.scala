/*
 * Copyright 2018 Kaya Kupferschmidt
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

import com.dimajix.flowman.execution.MappingGraphTest.DummyMappingSpec
import org.scalatest.FlatSpec
import org.scalatest.Matchers
import com.dimajix.flowman.model.{BaseMapping, BaseTarget, Mapping, MappingOutputIdentifier, Project, ResourceIdentifier, Target, TargetIdentifier, Template}
import org.apache.spark.sql.DataFrame

class TargetOrderingWithCacheTest extends FlatSpec with Matchers {
    case class DummyTarget(
        override val context: Context,
        override val name: String,
        mappings: Set[MappingOutputIdentifier]
    ) extends BaseTarget {
        protected override def instanceProperties: Target.Properties = Target.Properties(context, name)

        /**
         * Returns all phases which are implemented by this target in the execute method
         *
         * @return
         */
        override def phases : Set[Phase] = Lifecycle.ALL.toSet

        override def mappings(phase: Phase): Set[MappingOutputIdentifier] = mappings
    }

    case class DummyTargetSpec(
        name: String,
        mappings: Set[MappingOutputIdentifier]
    ) extends Template[Target] {
        override def instantiate(context: Context): Target = DummyTarget(context, name, mappings)
    }

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


    "Ordering" should "work with simple resources" in {
        val project: Project = Project(
            "test",
            mappings = Map(
                "m1" -> DummyMappingSpec(
                    "m1",
                    Seq()
                ),
                "m2" -> DummyMappingSpec(
                    "m2",
                    Seq( MappingOutputIdentifier("m1") )
                ),
                "m3" -> DummyMappingSpec(
                    "m3",
                    Seq()
                ),
                "m4" -> DummyMappingSpec(
                    "m4",
                    Seq()
                ),
                "m5" -> DummyMappingSpec(
                    "m5",
                    Seq()
                )
            ),
            targets = Map(
                "a" -> DummyTargetSpec("a",
                    Set(
                        MappingOutputIdentifier("m1")
                    )
                ),
                "b" -> DummyTargetSpec("b",
                    Set(
                        MappingOutputIdentifier("m2")
                    )
                ),
                "c" -> DummyTargetSpec("c",
                    Set(
                        MappingOutputIdentifier("m2"), MappingOutputIdentifier("m3")
                    )
                ),
                "d" -> DummyTargetSpec("d",
                    Set(
                        MappingOutputIdentifier("m3")
                    )
                ),
                "e" -> DummyTargetSpec("e",
                    Set(
                        MappingOutputIdentifier("m3"), MappingOutputIdentifier("m4")
                    )
                ),
                "f" -> DummyTargetSpec("f",
                    Set(
                        MappingOutputIdentifier("m4")
                    )
                )
            )
        )

        val session = Session.builder.build
        val context = session.getContext( project )
        val targets = project.targets.keys.map( targetName => context.getTarget( TargetIdentifier( targetName ) ) ).toSeq

        val ordering = new TargetOrderingWithCache
        val ordered = ordering
            .sort( targets, Phase.BUILD )
            .map( target => {
                target
                    .name
                    .split("_")
                    .slice( 0, 2 )
                    .mkString("_")
            })

        ordered.foreach(println)
        ordered should be ( Seq(
            "cached_m1",
            "a",
            "cached_m2",
            "uncached_m1",
            "b",
            "cached_m3",
            "c",
            "uncached_m2",
            "d",
            "cached_m4",
            "e",
            "uncached_m3",
            "f",
            "uncached_m4"
        ))
    }
}
