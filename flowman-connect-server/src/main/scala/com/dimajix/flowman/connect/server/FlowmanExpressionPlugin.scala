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

import java.util.Optional

import org.apache.spark.connect.proto
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.connect.planner.SparkConnectPlanner
import org.apache.spark.sql.connect.plugin.ExpressionPlugin
import org.slf4j.LoggerFactory

import com.dimajix.flowman.connect.proto.FlowmanNullableStruct
import com.dimajix.spark.sql.expressions.CreateNullableNamedStruct


/**
  * ExpressionPlugin for the Flowman Spark Connect server plugin.
  *
  * Supported extension expressions:
  *
  *  - [[FlowmanNullableStruct]]: creates the classic `nullable_struct` expression (a struct which evaluates to NULL
  *    iff all children evaluate to NULL), including the propagation of column metadata from named children into the
  *    struct field metadata - which the public Spark Connect API cannot express.
  */
class FlowmanExpressionPlugin extends ExpressionPlugin {
    private val logger = LoggerFactory.getLogger(classOf[FlowmanExpressionPlugin])

    override def transform(bytes:Array[Byte], planner:SparkConnectPlanner) : Optional[Expression] = {
        val msg = FlowmanPluginPayloads.decode(bytes)
        msg match {
            case msg:FlowmanNullableStruct => Optional.of(nullableStruct(planner, msg))
            case other =>
                logger.debug(s"Ignoring non-Flowman expression extension: ${other.typeUrl}")
                Optional.empty()
        }
    }


    private def nullableStruct(planner:SparkConnectPlanner, msg:FlowmanNullableStruct) : Expression = {
        if (msg.names.length != msg.children.length)
            throw new IllegalArgumentException("FlowmanNullableStruct requires exactly one name per child expression")

        // Transform each serialized child expression into a Catalyst expression, then build the classic
        // CreateNullableNamedStruct. The children may contain unresolved attributes, which are resolved during
        // analysis once the expression is placed into the context of the input relation.
        val children = msg.children.map { child =>
            val expr = proto.Expression.parseFrom(child)
            planner.transformExpression(expr)
        }
        val input = msg.names.zip(children).flatMap { case (name, child) => Seq(Literal(name), child) }
        CreateNullableNamedStruct(input)
    }
}
