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

package com.dimajix.flowman.execution

import java.util.Collections
import java.util.WeakHashMap

import scala.util.control.NonFatal

import org.apache.spark.connect.proto
import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connect.ConnectClientBridge
import org.apache.spark.sql.connect.ConnectConversions
import org.apache.spark.sql.connect.ColumnNodeToProtoConverter
import org.apache.spark.sql.types.DataType
import org.slf4j.LoggerFactory
import org.sparkproject.connect.protobuf.Any
import org.sparkproject.connect.protobuf.ByteString

import com.dimajix.flowman.connect.proto.FlowmanCapabilities
import com.dimajix.flowman.connect.proto.FlowmanCountRecords
import com.dimajix.flowman.connect.proto.FlowmanDescribe
import com.dimajix.flowman.connect.proto.FlowmanFsCommand
import com.dimajix.flowman.connect.proto.FlowmanMessage
import com.dimajix.flowman.connect.proto.FlowmanMessages
import com.dimajix.flowman.connect.proto.FlowmanNullableStruct
import com.dimajix.flowman.connect.proto.FlowmanPing
import com.dimajix.flowman.types.StructType


/**
  * Client-side gateway to the Flowman Spark Connect server plugin (see module "flowman-connect-server"). The plugin
  * restores classic-like behavior on the server for operations which cannot be expressed via the public Spark
  * Connect client API:
  *
  *   - enriched "describe" results (CHAR/VARCHAR lineage and column descriptions, [[FlowmanDescribe]])
  *   - server-side row counting during plan execution ([[FlowmanCountRecords]])
  *   - the `nullable_struct` expression incl. metadata propagation ([[FlowmanNullableStruct]])
  *   - data source capability probes (multi-path support, via [[FlowmanCapabilities]])
  *   - server-side file system operations ([[FlowmanFsCommand]])
  *
  * All operations degrade gracefully: when the server plugin is not installed (or is not version compatible), the
  * callers fall back to the public client API variants. The detected capabilities are cached per [[SparkSession]]
  * to avoid repeating the handshake on every call.
  */
object ConnectServerPlugin {
    private val logger = LoggerFactory.getLogger(getClass)

    /** The capabilities of a compatible Flowman server plugin */
    final case class Capabilities(flowmanVersion:String, apiVersion:String)

    private val sessionCache = Collections.synchronizedMap(new WeakHashMap[SparkSession, Option[Capabilities]]())


    /**
      * Returns the capabilities of the Flowman server plugin, if it is installed on the server side and the
      * protocol versions match. Returns `None` if the plugin is unavailable or incompatible.
      */
    def capabilities(spark:SparkSession) : Option[Capabilities] = {
        val cached = sessionCache.get(spark)
        if (cached != null) {
            cached
        }
        else {
            val detected = detect(spark)
            sessionCache.put(spark, detected)
            detected
        }
    }

    /**
      * Returns true when a compatible Flowman server plugin is detected.
      */
    def isAvailable(spark:SparkSession) : Boolean = capabilities(spark).isDefined


    private def detect(spark:SparkSession) : Option[Capabilities] = {
        try {
            val connectSession = ConnectConversions.castToImpl(spark)
            val msg = FlowmanCapabilities(FlowmanMessages.FLOWMAN_VERSION, FlowmanMessages.API_VERSION, None)
            val df = connectSession.newDataFrame { builder =>
                builder.setExtension(packAny(msg))
            }
            val rows = df.collect()
            if (rows.length != 1) {
                logger.info("Flowman server plugin did not return a capabilities row; treating as unavailable")
                None
            }
            else {
                val row = rows(0)
                val serverVersion = row.getString(0)
                val apiVersion = row.getString(1)
                if (apiVersion == FlowmanMessages.API_VERSION) {
                    logger.info(s"Detected Flowman server plugin version $serverVersion (API $apiVersion)")
                    Some(Capabilities(serverVersion, apiVersion))
                }
                else {
                    logger.warn(
                        s"Flowman server plugin version mismatch: client expects API ${FlowmanMessages.API_VERSION}, " +
                            f"server provides API ${apiVersion}. Disabling plugin features."
                    )
                    None
                }
            }
        }
        catch {
            case NonFatal(ex) =>
                logger.info(
                    s"Flowman server plugin not available on the connected Spark server: ${ex.getMessage}. " +
                        s"Degrading to public Spark Connect API."
                )
                None
        }
    }


    /**
      * Enriched describe for a Connect client. When the server plugin is available, the "described" schema of the
      * given DataFrame is computed on the server (recovering CHAR/VARCHAR lengths and column descriptions from the
      * analyzed plan). Otherwise the plain df.schema is used.
      */
    def describe(execution:Execution, df:DataFrame) : StructType = {
        capabilities(execution.spark) match {
            case Some(_) =>
                describeRemote(execution, df)
            case None =>
                StructType.of(df.schema)
        }
    }

    private def describeRemote(execution:Execution, df:DataFrame) : StructType = {
        val connectSession = ConnectConversions.castToImpl(execution.spark)
        val plan = ConnectClientBridge.plan(ConnectConversions.castToImpl(df))
        val msg = FlowmanDescribe(plan.getRoot.toByteArray)
        val resultDf = connectSession.newDataFrame { builder =>
            builder.setExtension(packAny(msg))
        }
        val json = resultDf.collect().head.getString(0)
        val sparkSchema = DataType.fromJson(json).asInstanceOf[org.apache.spark.sql.types.StructType]
        StructType.of(sparkSchema)
    }


    /**
      * Returns the `nullable_struct` expression using the server plugin, which introduces a struct that evaluates
      * to NULL iff all children are NULL - including classic-style metadata propagation. Returns `None` when the
      * server plugin is not available; the caller then falls back to the public API variant
      * ([[com.dimajix.spark.sql.functions.nullable_struct]]).
      */
    def nullableStruct(spark:SparkSession, cols:Seq[Column]) : Option[Column] = {
        if (cols.isEmpty) {
            None
        }
        else {
            capabilities(spark) match {
                case Some(_) =>
                    val names = cols.map(ConnectClientBridge.fieldName)
                    val children = cols.map(col => ColumnNodeToProtoConverter.toExpr(col).toByteArray)
                    val msg = FlowmanNullableStruct(names, children)
                    val expr = proto.Expression.newBuilder()
                        .setExtension(packAny(msg))
                        .build()
                    Some(ConnectConversions.column(expr))
                case None =>
                    None
            }
        }
    }


    /**
      * Wraps the given DataFrame (which is used for an insert/overwrite) in a server-side CountRecords node, so
      * that the number of records is counted during plan execution rather than via an extra action.
      */
    def countRecords(spark:SparkSession, df:DataFrame) : Option[DataFrame] = {
        capabilities(spark) match {
            case Some(_) =>
                val connectSession = ConnectConversions.castToImpl(spark)
                val plan = ConnectClientBridge.plan(ConnectConversions.castToImpl(df))
                val msg = FlowmanCountRecords(plan.getRoot.toByteArray)
                Some(connectSession.newDataFrame { builder =>
                    builder.setExtension(packAny(msg))
                })
            case None =>
                None
        }
    }


    /**
      * Probes the server for multi-path support of the given data source format. Returns `None` when the server
      * plugin is not available, so that the caller can fall back to its static assumption.
      */
    def relationSupportsMultiplePaths(spark:SparkSession, format:String) : Option[Boolean] = {
        capabilities(spark) match {
            case Some(_) =>
                try {
                    val connectSession = ConnectConversions.castToImpl(spark)
                    val msg = FlowmanCapabilities(FlowmanMessages.FLOWMAN_VERSION, FlowmanMessages.API_VERSION, Some(format))
                    val df = connectSession.newDataFrame { builder =>
                        builder.setExtension(packAny(msg))
                    }
                    val rows = df.collect()
                    if (rows.length == 1)
                        Some(rows(0).getBoolean(2))
                    else
                        None
                }
                catch {
                    case NonFatal(ex) =>
                        logger.info(s"Capability probe for format '$format' failed: ${ex.getMessage}")
                        None
                }
            case None =>
                None
        }
    }


    /**
      * Version handshake via the server-side CommandPlugin. Returns true when the plugin is present and the API
      * versions match. This is a pure side-effect round trip; for feature detection the [[capabilities]] method
      * (which also returns data) should be preferred.
      */
    def ping(spark:SparkSession) : Boolean = {
        try {
            val connectSession = ConnectConversions.castToImpl(spark)
            val msg = FlowmanPing(FlowmanMessages.FLOWMAN_VERSION, FlowmanMessages.API_VERSION)
            val command = proto.Command.newBuilder()
                .setExtension(packAny(msg))
                .build()
            val plan = proto.Plan.newBuilder()
                .setCommand(command)
                .build()
            ConnectClientBridge.execute(connectSession, plan)
            true
        }
        catch {
            case NonFatal(_) => false
        }
    }

    /**
      * Runs a file system operation on the server side. Returns true when the server plugin handled the command.
      */
    def fsOperation(spark:SparkSession, operation:String, path:String) : Boolean = {
        try {
            val connectSession = ConnectConversions.castToImpl(spark)
            val msg = FlowmanFsCommand(operation, path)
            val command = proto.Command.newBuilder()
                .setExtension(packAny(msg))
                .build()
            val plan = proto.Plan.newBuilder()
                .setCommand(command)
                .build()
            ConnectClientBridge.execute(connectSession, plan)
            true
        }
        catch {
            case NonFatal(ex) =>
                logger.info(s"Server-side file system operation '$operation' on '$path' failed: ${ex.getMessage}")
                false
        }
    }

    private def packAny(msg:FlowmanMessage) : Any = {
        Any.newBuilder()
            .setTypeUrl(msg.typeUrl)
            .setValue(ByteString.copyFrom(msg.toBytes))
            .build()
    }
}
