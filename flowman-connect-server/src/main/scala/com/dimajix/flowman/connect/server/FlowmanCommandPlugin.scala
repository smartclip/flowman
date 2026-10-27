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

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
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
  *  - [[FlowmanFsCommand]]: executes file system operations (currently "mkdirs" and "delete") on the server driver,
  *    e.g. for platforms where the client has no direct access to the cluster storage.
  */
class FlowmanCommandPlugin extends CommandPlugin {
    private val logger = LoggerFactory.getLogger(classOf[FlowmanCommandPlugin])

    override def process(bytes:Array[Byte], planner:SparkConnectPlanner) : Boolean = {
        val msg = FlowmanPluginPayloads.decode(bytes)
        msg match {
            case msg:FlowmanPing => ping(msg)
            case msg:FlowmanFsCommand => fsCommand(planner, msg)
            case other =>
                logger.debug(s"Ignoring non-Flowman command extension: ${other.typeUrl}")
                false
        }
    }


    private def ping(msg:FlowmanPing) : Boolean = {
        if (msg.apiVersion != FlowmanMessages.API_VERSION) {
            logger.warn(
                s"Flowman Connect client uses protocol version ${msg.apiVersion}, but the server supports " +
                    s"${FlowmanMessages.API_VERSION}. Plugin features will be disabled on the client."
            )
        }
        true
    }


    private def fsCommand(planner:SparkConnectPlanner, msg:FlowmanFsCommand) : Boolean = {
        val hadoopConf = planner.session.sessionState.newHadoopConf()
        val path = new Path(msg.path)
        val fs = path.getFileSystem(hadoopConf)
        val success = msg.operation match {
            case "mkdirs" => fs.mkdirs(path)
            case "delete" => fs.delete(path, true)
            case other => throw new IllegalArgumentException(s"Unsupported file system operation: $other")
        }
        if (!success) {
            throw new java.io.IOException(s"File system operation '${msg.operation}' on '${msg.path}' reported failure")
        }
        true
    }
}
