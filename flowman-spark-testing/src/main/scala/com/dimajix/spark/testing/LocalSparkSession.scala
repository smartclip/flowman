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

import java.io.File

import scala.util.control.NonFatal

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hive.common.util.HiveVersionInfo
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hive.HiveClientAccessor
import org.scalatest.Suite

import LocalSparkSession.logger


object LocalSparkSession {
    private val logger = org.slf4j.LoggerFactory.getLogger(classOf[LocalSparkSession])

    /**
      * A single embedded Derby metastore shared by all suites of one JVM in Connect mode.
      *
      * The in-process Spark Connect server runs on a JVM-wide gRPC thread pool, and Hive caches its
      * metastore client (and session state) in JVM-wide thread-locals. With a per-suite metastore a
      * reused gRPC thread could keep talking to the previous suite's Derby database, making tables
      * created in the current suite invisible. Sharing one metastore for the whole JVM removes that
      * divergence. (Classic mode keeps one metastore per suite, since Spark creates a new context
      * and executor threads for every suite anyway.)
      */
    private[testing] lazy val sharedMetastorePath : String = {
        // NOTE: the directory must not exist yet, Derby creates the database directory itself.
        val dir = new java.io.File(System.getProperty("java.io.tmpdir"), "flowman-metastore-" + java.util.UUID.randomUUID())
        dir.deleteOnExit()
        dir.getCanonicalPath
    }
}

/**
  * A local Spark test fixture. By default it creates a classic SparkSession. When the system property
  * "spark.api.mode=connect" is set (as done by the "spark-connect" Maven profile), it instead starts an in-process
  * Spark Connect server and creates a Spark Connect client session. In that case `spark` refers to the Connect
  * session and any access to `sc` (i.e. `spark.sparkContext`) throws, exactly like on a remote endpoint.
  */
trait LocalSparkSession extends LocalTempDir { this:Suite =>
    private var _spark: Option[SparkSession] = None
    private var _sc: Option[SparkContext] = None
    private var _serverSpark: Option[SparkSession] = None

    lazy val conf = new SparkConf(false)
    def spark : SparkSession = _spark.getOrElse(throw new IllegalStateException("No active Spark session"))
    def sc : SparkContext = _sc.getOrElse(spark.sparkContext)

    val hiveSupported: Boolean = try {
          org.apache.hadoop.hive.shims.ShimLoader.getMajorVersion
          true
        }
        catch {
            case _: ClassNotFoundException => false
            case _: NoClassDefFoundError => false
            case NonFatal(_) => false
        }

    /**
      * Returns true if the Spark Connect fixture should be used. This is the case when the system property
      * "spark.api.mode=connect" is set, or when the [[LocalConnectSparkSession]] trait is mixed in.
      */
    def isSparkConnectEnabled: Boolean = LocalConnectSparkSession.isConnectEnabled

    def configureSpark(builder: SparkSession.Builder) : SparkSession.Builder = {
        builder
    }

    /**
      * Configures the server-side SparkSession of the in-process Spark Connect server. Override this hook in tests
      * to register server-side plugins/extensions (e.g. the Flowman Spark Connect server plugin). By default the
      * Flowman server plugin is registered automatically whenever its classes are on the (test) classpath.
      */
    def configureServerSpark(builder: SparkSession.Builder) : SparkSession.Builder = {
        // Automatically register the Flowman Spark Connect server plugin (its SparkSessionExtensions plus
        // RelationPlugin/ExpressionPlugin/CommandPlugin classes) whenever the flowman-connect-server module is on
        // the (test) classpath. Reflection is used here so that this fixture module does not depend on it.
        try {
            builder
                .config("spark.sql.extensions", Class.forName("com.dimajix.flowman.connect.server.FlowmanSparkSessionExtensions").getName)
                .config("spark.connect.extensions.relation.classes", Class.forName("com.dimajix.flowman.connect.server.FlowmanRelationPlugin").getName)
                .config("spark.connect.extensions.expression.classes", Class.forName("com.dimajix.flowman.connect.server.FlowmanExpressionPlugin").getName)
                .config("spark.connect.extensions.command.classes", Class.forName("com.dimajix.flowman.connect.server.FlowmanCommandPlugin").getName)
        }
        catch {
            case _: ClassNotFoundException => builder
        }
    }

    override def beforeAll() : Unit = {
        super.beforeAll()

        val localMetastorePath = if (isSparkConnectEnabled)
            LocalSparkSession.sharedMetastorePath
        else
            new File(tempDir, "metastore").getCanonicalPath
        val localWarehousePath = new File(tempDir, "warehouse").getCanonicalPath
        val checkpointPath  = new File(tempDir, "checkpoints").getCanonicalPath
        val streamingCheckpointPath  = new File(tempDir, "streamingCheckpoints").getCanonicalPath

        if (isSparkConnectEnabled) {
            bootstrapConnect(
                localMetastorePath,
                localWarehousePath,
                checkpointPath,
                streamingCheckpointPath
            )
        }
        else {
            bootstrapClassic(
                localMetastorePath,
                localWarehousePath,
                checkpointPath,
                streamingCheckpointPath
            )
        }
    }

    private def baseBuilder(
        localMetastorePath:String,
        localWarehousePath:String,
        streamingCheckpointPath:String
    ) : SparkSession.Builder = {
        val builder = SparkSession.builder()
            .master("local[4]")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "8")
            .config("spark.sql.session.timeZone", "UTC")

        // Only enable Hive support when it actually works. Currently Spark 2.x will not support Hadoop 3.x
        if (hiveSupported) {
            // We have to mask all properties in hive-site.xml that relates to metastore
            // data source as we used a local metastore here.
            val hiveConfVars = HiveConf.ConfVars.values()
            hiveConfVars.foreach { confvar =>
                if (confvar.varname.contains("datanucleus") ||
                    confvar.varname.contains("jdo")) {
                    builder.config("spark.hadoop." + confvar.varname, confvar.getDefaultExpr())
                }
            }
            builder.config("spark.hadoop.javax.jdo.option.ConnectionURL", s"jdbc:derby:;databaseName=$localMetastorePath;create=true")
                .config("spark.hadoop.datanucleus.connectionPool", "None")
                .config("spark.hadoop.datanucleus.connectionPoolingType", "None")
                .config("spark.hadoop.datanucleus.connectionPoolingType.nontx", "None")
                .config("spark.hadoop.datanucleus.retainValues", true)
                .config("spark.hadoop.datanucleus.rdbms.datastoreAdapterClassName", "org.datanucleus.store.rdbms.adapter.DerbyAdapter")
                .config("spark.hadoop.datanucleus.schema.autoCreateTables", true)
                .config("spark.hadoop.datanucleus.schema.autoCreateAll", true)
                .config("spark.hadoop.datanucleus.autoCreateSchema", true)
                .config("spark.hadoop.datanucleus.autoCreateColumns", true)
                .config("spark.hadoop.datanucleus.autoCreateConstraints", true)
                .config("spark.hadoop.datanucleus.autoStartMechanismMode", "ignored")
                .config("spark.hadoop.hive.metastore.schema.verification.record.version", true)
                .config("spark.hadoop.hive.metastore.schema.verification", false)
                .config("spark.hadoop.hive.metastore.uris", "")
                .config("spark.sql.hive.metastore.sharedPrefixes", "nothing.here")

            // Either way, we get some warnings, depending on the Hive features being used
            val version = HiveVersionInfo.getShortVersion.split('.')
            if (version(0).toInt >= 3)
                builder.config("spark.hadoop.hive.metastore.try.direct.sql", false)
            else
                builder.config("spark.hadoop.hive.metastore.try.direct.sql", true)

            builder.enableHiveSupport()
        }

        builder.config("spark.sql.streaming.checkpointLocation", streamingCheckpointPath)
            .config("spark.sql.warehouse.dir", localWarehousePath)
            .config(conf)

        builder
    }

    private def bootstrapClassic(
        localMetastorePath:String,
        localWarehousePath:String,
        checkpointPath:String,
        streamingCheckpointPath:String
    ) : Unit = {
        val builder = baseBuilder(localMetastorePath, localWarehousePath, streamingCheckpointPath)

        configureSpark(builder)

        val spark = builder.getOrCreate()
        val sc = spark.sparkContext
        sc.setLogLevel("WARN")
        sc.setCheckpointDir(checkpointPath)

        _spark = Some(spark)
        _sc = Some(sc)

        // Perform one Spark operation, this help to fix some race conditions with frequent setup/teardown
        spark.emptyDataFrame.count()
    }

    private def bootstrapConnect(
        localMetastorePath:String,
        localWarehousePath:String,
        checkpointPath:String,
        streamingCheckpointPath:String
    ) : Unit = {
        // Create a classic session which acts as the server side session of the in-process Connect server.
        // NOTE: `configureSpark` is intentionally NOT applied here, it is an extension hook for the client session.
        // `configureServerSpark` on the other hand is applied to the server session, so that server-side plugins
        // and extensions (like the Flowman server plugin) can be registered.
        val serverBuilder = baseBuilder(localMetastorePath, localWarehousePath, streamingCheckpointPath)
            .config("spark.connect.grpc.binding.port", "0")
            // Bind the in-process Connect server to the IPv4 loopback address, so that the client can reach it via
            // "localhost". Without this hint Netty may bind to the IPv6 wildcard address, which is not reachable
            // when the client resolves localhost to 127.0.0.1.
            .config("spark.connect.grpc.binding.address", "127.0.0.1")

        configureServerSpark(serverBuilder)

        val serverSpark = serverBuilder.getOrCreate()
        serverSpark.sparkContext.setLogLevel("WARN")
        _serverSpark = Some(serverSpark)

        // Start the in-process Spark Connect server
        org.apache.spark.sql.connect.service.SparkConnectService.start(serverSpark.sparkContext)
        // The server binds to an arbitrary (or wildcard) address. Determine a client-reachable host: use loopback
        // for wildcard bindings ("0.0.0.0" or "::"), otherwise use the bound interface address.
        val connectAddress = LocalConnectSparkSession.connectAddress
        val connectHost = {
            val isAnyLocal = Option(connectAddress.getAddress).exists(_.isAnyLocalAddress)
            if (isAnyLocal) "localhost" else connectAddress.getHostString
        }
        val connectUrl = s"sc://$connectHost:${connectAddress.getPort}"
        // Publish the in-process Connect server also as a system property, so that any SparkSession built from a
        // fresh SparkConf (which loads "spark.*" system properties, e.g. via Configuration.sparkConf) finds it.
        // Without this, a fresh builder in Connect mode falls back to the default endpoint "localhost:15002" and
        // Spark's default client retry policy retries forever against it.
        System.setProperty("spark.connect.remote", connectUrl)
        // NOTE: the client session is created AFTER this property is set, so builders in this JVM see the server.
        logger.info(s"Spark Connect server listening on ${connectHost}:${connectAddress.getPort}")
        logger.info(s"Spark Connect server listening on ${connectHost}:${connectAddress.getPort}")
        // Create the Spark Connect client session. Note that all server-side settings (Hive
        // metastore, warehouse, session time zone, executor sizing, ...) are already configured on
        // the server session above, so the client only needs the connection details. Static configs
        // like "spark.sql.warehouse.dir" cannot be changed from a Connect client and must not be
        // set here. In particular the test JVM sets "spark.sql.warehouse.dir" as a system property
        // (scalatest-maven-plugin), which the Connect client builder otherwise auto-loads into its
        // SparkConf and pushes to the server as a `config` RPC, failing with
        // CANNOT_MODIFY_STATIC_CONFIG. The server already has the correct value, so the client
        // simply must not carry the property.
        val warehouseDirProperty = System.getProperty("spark.sql.warehouse.dir")
        if (warehouseDirProperty != null)
            System.clearProperty("spark.sql.warehouse.dir")
        val clientBuilder = SparkSession.builder()
            .config("spark.api.mode", "connect")
            // NOTE: The SparkSessionBuilder reads the remote connection string from
            // "spark.connect.remote", NOT from "spark.remote".
            .config("spark.connect.remote", s"sc://$connectHost:${connectAddress.getPort}")
            .config("spark.sql.shuffle.partitions", "8")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.streaming.checkpointLocation", streamingCheckpointPath)
            .config(conf)

        configureSpark(clientBuilder)

        // Spark Connect's `getOrCreate()` returns a stopped-but-still-active session, after which every RPC
        // fails forever ("Channel shutdown invoked"). A previous suite's client session may still be active
        // (its shutdown can be asynchronous), so never reuse it - always create a fresh client session. Only
        // clear a stale Connect session; the classic server session must stay active for the in-process
        // Connect server to create isolated sessions.
        val staleConnectSession = SparkSession.getActiveSession.exists(_.getClass.getName == "org.apache.spark.sql.connect.SparkSession")
        if (staleConnectSession) {
            SparkSession.clearActiveSession()
            SparkSession.clearDefaultSession()
        }
        val spark = clientBuilder.getOrCreate()

        _spark = Some(spark)
        _sc = None
    }

    override def afterAll() : Unit = {
       _spark.foreach { spark =>
            if (_serverSpark.isEmpty && hiveSupported) {
                // Newer version of Hive have background thread-pools which screw up the local Derby database
                // when not properly shut down
                HiveClientAccessor.withHiveState(spark) {
                    try {
                        val clazz = Class.forName("org.apache.hadoop.hive.metastore.ThreadPool")
                        val method = clazz.getMethod("shutdown")
                        method.invoke(clazz)
                    }
                    catch {
                        case _:ClassNotFoundException =>
                        case _:NoSuchMethodException =>
                    }
                }
            }

            spark.stop()
            _spark = None
            _sc = None
        }
       if (_serverSpark.nonEmpty) {
            org.apache.spark.sql.connect.service.SparkConnectService.stop()
            _serverSpark.foreach { spark =>
                spark.stop()
                _serverSpark = None
            }
        }

        super.afterAll()
    }
}
