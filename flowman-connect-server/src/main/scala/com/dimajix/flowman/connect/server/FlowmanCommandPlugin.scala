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

import org.apache.spark.sql.connect.planner.SparkConnectPlanner
import org.apache.spark.sql.connect.plugin.CommandPlugin
import org.slf4j.LoggerFactory

import com.dimajix.flowman.connect.proto.FlowmanFsCommand
import com.dimajix.flowman.connect.proto.FlowmanMessages
import com.dimajix.flowman.connect.proto.FlowmanPing


/**
  * CommandPlugin for the Flowman Spark Connect server plugin.
  *
  * Supported extension commands:
  *
  *  - [[FlowmanPing]]: version handshake. Always succeeds; the client uses it to detect the plugin and to log a
  *    warning when the protocol versions of client and server differ.
  *
  * File system commands are deliberately not executed: any Connect client could otherwise delete arbitrary paths
  * with the server's credentials. The corresponding message type is retained for protocol compatibility only.
  */
class FlowmanCommandPlugin extends CommandPlugin {
    private val logger = LoggerFactory.getLogger(classOf[FlowmanCommandPlugin])

    override def process(bytes:Array[Byte], planner:SparkConnectPlanner) : Boolean = {
        FlowmanPluginPayloads.decode(bytes) match {
            case Some(msg:FlowmanPing) => ping(msg)
            case Some(msg:FlowmanFsCommand) =>
                logger.warn("Rejecting FlowmanFsCommand: server-side file system operations are disabled")
                false
            case Some(other) =>
                logger.debug(s"Ignoring non-Flowman command extension: ${other.typeUrl}")
                false
            case None =>
                false
        }
    }


    private def ping(msg:FlowmanPing) : Boolean = {
        if (msg.apiVersion != FlowmanMessages.API_VERSION) {
            logger.warn(
                s"Flowman Connect client uses protocol version ${msg.apiVersion}, but the server supports " +
                    s"${FlowmanMessages.API_VERSION}. Plugin features will be disabled on the client."
            )
            // Fail the command so that the client-side ping reports an incompatible plugin
            throw new IllegalArgumentException(
                s"Incompatible Flowman Connect protocol version ${msg.apiVersion} (server: ${FlowmanMessages.API_VERSION})"
            )
        }
        true
    }
}
