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

import scala.util.control.NonFatal

import org.apache.spark.connect.proto
import org.apache.spark.sql.SparkShim
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connect.planner.SparkConnectPlanner
import org.apache.spark.sql.connect.plugin.RelationPlugin
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.types.StringType
import org.slf4j.LoggerFactory

import com.dimajix.flowman.connect.proto.FlowmanCapabilities
import com.dimajix.flowman.connect.proto.FlowmanCountRecords
import com.dimajix.flowman.connect.proto.FlowmanDescribe
import com.dimajix.flowman.connect.proto.FlowmanMessages
import com.dimajix.flowman.connect.proto.FlowmanMessage
import com.dimajix.flowman.model.SchemaExtractor
import com.dimajix.spark.sql.catalyst.plans.logical.CountRecords


/**
  * RelationPlugin for the Flowman Spark Connect server plugin.
  *
  * Supported extension relations (see /workspace/flowman/flowman-connect-common):
  *
  *  - [[FlowmanCapabilities]]: reports the server Flowman version, the supported protocol version and - when
  *    requested for a data source format - whether that format supports reading multiple paths at once.
  *  - [[FlowmanDescribe]]: computes the enriched "described" schema (CHAR/VARCHAR lengths and column descriptions
  *    derived from the analyzed plan, like the classic backend) of an input plan and returns it as JSON.
  *  - [[FlowmanCountRecords]]: wraps the input plan in a classic `CountRecords` node so that the number of records
  *    is counted into a server-side accumulator during plan execution (requires the [[FlowmanSparkSessionExtensions]]
  *    for planning).
  */
class FlowmanRelationPlugin extends RelationPlugin {
    private val logger = LoggerFactory.getLogger(classOf[FlowmanRelationPlugin])

    override def transform(bytes:Array[Byte], planner:SparkConnectPlanner) : Optional[LogicalPlan] = {
        FlowmanPluginPayloads.decode(bytes) match {
            case Some(msg:FlowmanCapabilities) => Optional.of(features(planner, msg))
            case Some(msg:FlowmanDescribe) => Optional.of(describe(planner, msg))
            case Some(msg:FlowmanCountRecords) => Optional.of(countRecords(planner, msg))
            case Some(other) =>
                logger.debug(s"Ignoring non-Flowman relation extension: ${other.typeUrl}")
                Optional.empty()
            case None =>
                Optional.empty()
        }
    }


    private def features(planner:SparkConnectPlanner, msg:FlowmanCapabilities) : LogicalPlan = {
        msg.format match {
            case Some(format) =>
                val supportsMultiplePaths = try {
                    SparkShim.relationSupportsMultiplePaths(planner.session, format)
                }
                catch {
                    case NonFatal(ex) =>
                        logger.warn(s"Could not determine multi-path support for format '$format': ${ex.getMessage}")
                        false
                }
                LocalRelation.fromProduct(
                    output = Seq(
                        AttributeReference("flowman_version", StringType, nullable = false)(),
                        AttributeReference("api_version", StringType, nullable = false)(),
                        AttributeReference("supports_multiple_paths", BooleanType, nullable = false)()
                    ),
                    data = Seq((FlowmanMessages.FLOWMAN_VERSION, FlowmanMessages.API_VERSION, supportsMultiplePaths))
                )
            case None =>
                LocalRelation.fromProduct(
                    output = Seq(
                        AttributeReference("flowman_version", StringType, nullable = false)(),
                        AttributeReference("api_version", StringType, nullable = false)()
                    ),
                    data = Seq((FlowmanMessages.FLOWMAN_VERSION, FlowmanMessages.API_VERSION))
                )
        }
    }


    private def describe(planner:SparkConnectPlanner, msg:FlowmanDescribe) : LogicalPlan = {
        val relation = proto.Relation.parseFrom(msg.input)
        val plan = planner.transformRelation(relation)
        val analyzed = planner.session.sessionState.executePlan(plan).analyzed

        // Compute the enriched schema (CHAR/VARCHAR lengths + column descriptions) like the classic backend would
        // and serialize it as the Spark schema JSON format (incl. metadata), which `org.apache.spark.sql.types.
        // DataType.fromJson` (and hence the Flowman client gateway) can parse back.
        val schema = SchemaExtractor.extractSchema(analyzed)
        val json = SchemaJson.encode(schema.sparkType)

        LocalRelation.fromProduct(
            output = Seq(AttributeReference("schema", StringType, nullable = false)()),
            data = Seq(Tuple1(json))
        )
    }


    private def countRecords(planner:SparkConnectPlanner, msg:FlowmanCountRecords) : LogicalPlan = {
        val relation = proto.Relation.parseFrom(msg.input)
        val plan = planner.transformRelation(relation)
        val counter = planner.session.sparkContext.longAccumulator
        CountRecords(plan, counter)
    }
}
