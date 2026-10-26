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

import org.scalatest.Suite


object LocalConnectSparkSession {
    /**
      * Determines whether Spark Connect mode is enabled for the local test session. This can either be selected
      * by setting the system property "spark.api.mode=connect" (used by the "spark-connect" Maven profile) or
      * by mixing in the [[LocalConnectSparkSession]] trait explicitly.
      */
    def isConnectEnabled : Boolean =
        System.getProperty("spark.api.mode") == "connect" || System.getProperty("flowman.test.connect") == "true"

    /**
      * Returns the actually bound TCP port of the in-process Spark Connect server. The public accessors of
      * [[org.apache.spark.sql.connect.service.SparkConnectService]] (`localPort`, `bindingAddress`) are marked
      * `private[connect]` in Spark, so we read them via reflection. The methods are public in the bytecode, which
      * makes this robust enough for a test fixture. `bindingAddress` returns the *configured* bind address (which may
      * still show the configured port before the server is bound), so the actually bound port is read via `localPort`.
      */
    def connectAddress : java.net.InetSocketAddress = {
        val service = org.apache.spark.sql.connect.service.SparkConnectService
        val bindingAddressMethod = service.getClass.getMethod("bindingAddress")
        val bindingAddress = bindingAddressMethod.invoke(service).asInstanceOf[java.net.InetSocketAddress]
        val localPortMethod = service.getClass.getMethod("localPort")
        val localPort = localPortMethod.invoke(service).asInstanceOf[Int]
        val host = if (bindingAddress == null) "localhost" else bindingAddress.getHostString
        new java.net.InetSocketAddress(host, localPort)
    }

    /**
      * Returns the TCP port of the in-process Spark Connect server.
      */
    def connectPort : Int = connectAddress.getPort
}

/**
  * Spark Connect variant of [[LocalSparkSession]]. This fixture starts an in-process Spark Connect server (based on
  * a classic SparkSession which is only used server side) and provides a Spark Connect session as `spark`. All
  * calls which require the classic SparkContext (like `spark.sparkContext`) will throw an exception, as they do on a
  * real remote Spark Connect endpoint.
  */
trait LocalConnectSparkSession extends LocalSparkSession { this:Suite =>
    override def isSparkConnectEnabled: Boolean = true
    def connectPort: Int = LocalConnectSparkSession.connectPort
}
