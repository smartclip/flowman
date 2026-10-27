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

package org.apache.spark.sql.connect

import org.apache.spark.connect.proto
import org.apache.spark.sql.Column
import org.apache.spark.sql.connect.client.SparkConnectClient


/**
  * Bridge into Spark Connect client internals which are not accessible from the public API. Flowman places this
  * helper in the `org.apache.spark.sql.connect` package (mirroring the strategy used by [[org.apache.spark.sql.
  * hive.HiveClientShim]]) so that it can access members which are only `private[sql]`.
  */
object ConnectClientBridge {
    /** The underlying gRPC client of a Spark Connect session. */
    def client(spark: SparkSession) : SparkConnectClient = spark.client

    /** The serialized `proto.Plan` of a Spark Connect DataFrame. */
    def plan(df: org.apache.spark.sql.connect.Dataset[_]) : proto.Plan = df.plan

    /** Executes a command plan on the server and drains the response stream. */
    def execute(spark: SparkSession, plan: proto.Plan) : Unit = {
        val iterator = spark.client.execute(plan, None)
        try {
            while (iterator.hasNext)
                iterator.next()
        }
        finally {
            iterator.close()
        }
    }

    /**
      * Returns the field name of a Column expression, as used by `struct`-like functions: the name of a (possibly
      * multi-part) column alias, or - as a fallback - the SQL representation of the underlying expression. The plan
      * for the plain `Column.node.sql` of an aliased column includes the whole "expr AS name" text, which is not a
      * valid field name.
      */
    def fieldName(col: Column) : String = {
        import org.apache.spark.sql.internal.Alias
        col.node match {
            case a: Alias => a.name.mkString(".")
            case _ =>
                val text = col.node.sql
                if (text.nonEmpty) text else col.toString
        }
    }
}
