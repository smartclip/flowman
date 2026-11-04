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

package com.dimajix.spark.sql

import org.apache.spark.sql.SparkSession


object SparkSessionUtils {
    /**
      * Returns true if the given session is a Spark Connect client session. Spark does not expose a public API to
      * distinguish the two modes, so the concrete implementation class is checked.
      */
    def isConnect(spark:SparkSession) : Boolean = {
        spark.getClass.getName == "org.apache.spark.sql.connect.SparkSession"
    }
}
