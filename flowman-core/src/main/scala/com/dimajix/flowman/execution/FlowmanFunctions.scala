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

import org.apache.spark.sql.Column
import org.apache.spark.sql.SparkSession


/**
  * Spark column helpers which transparently use the Flowman server plugin in Spark Connect mode. When the plugin is
  * not available, they fall back to the public (Spark Connect compatible) Spark API.
  */
object FlowmanFunctions {
    /**
      * Creates a struct which evaluates to NULL iff all children evaluate to NULL. When the Flowman server plugin is
      * available in Spark Connect mode, this is compiled to the classic `named_nullable_struct` expression on the
      * server (including the propagation of column metadata), otherwise it falls back to the public API variant.
      *
      * @param spark  The SparkSession; `None` if not available (e.g. when building columns from schemas only)
      * @param cols   The columns to aggregate in the struct
      */
    def nullableStruct(spark:Option[SparkSession], cols:Column*) : Column = {
        spark match {
            case Some(s) if SparkBackend.of(s).isConnect =>
                ConnectServerPlugin.nullableStruct(s, cols).getOrElse(com.dimajix.spark.sql.functions.nullable_struct(cols:_*))
            case _ =>
                com.dimajix.spark.sql.functions.nullable_struct(cols:_*)
        }
    }
}
