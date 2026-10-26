/*
 * Copyright (C) 2018 The Flowman Authors
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

package com.dimajix.spark.testing

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


class LocalConnectSparkSessionTest extends AnyFlatSpec with Matchers with LocalConnectSparkSession {
    "A connect-mode SparkSession" should "be a real Connect session" in {
        spark.getClass.getName should be ("org.apache.spark.sql.connect.SparkSession")
    }

    "A connect-mode SparkSession" should "support basic DataFrame operations" in {
        spark.range(10).count() should be (10)
    }

    "A connect-mode SparkSession" should "support Spark SQL" in {
        val rows = spark.sql("SELECT 'x' AS a, 42 AS b").collect()
        rows should have size (1)
        rows(0).getAs[String]("a") should be ("x")
        rows(0).getAs[Int]("b") should be (42)
    }
}
