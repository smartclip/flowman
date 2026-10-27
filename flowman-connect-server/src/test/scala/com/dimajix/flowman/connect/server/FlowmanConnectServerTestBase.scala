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

import org.apache.spark.sql.connect.plugin.SparkConnectPluginRegistry
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.spark.testing.LocalConnectSparkSession


/**
  * Base trait for the Flowman Spark Connect server plugin tests. The tests run in Spark Connect mode against the
  * in-process Connect server, which has the Flowman server plugin registered (the [[LocalSparkSession.
  * configureServerSpark]] fixture hook reflection-registers the plugin classes of this module).
  */
trait FlowmanConnectServerTestBase extends AnyFlatSpec with Matchers with LocalConnectSparkSession with BeforeAndAfterAll {
    // Ensure that the statically cached Spark Connect plugin registry is re-initialized from the SparkEnv conf of
    // the upcoming test session. Without this, a previous test suite in the same JVM (with or without the Flowman
    // plugin) would leave the JVM-wide registry cache behind, reading a stale plugin configuration.
    override def beforeAll() : Unit = {
        SparkConnectPluginRegistry.reset()
        super.beforeAll()
    }
}
