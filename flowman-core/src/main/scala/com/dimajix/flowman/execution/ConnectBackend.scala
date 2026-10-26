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

package com.dimajix.flowman.execution

import java.util.Locale

import org.apache.hadoop.conf.{Configuration => HadoopConf}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkShim
import org.slf4j.LoggerFactory

import com.dimajix.flowman.config.Configuration
import com.dimajix.flowman.metric.MultiMetricBundle
import com.dimajix.flowman.metric.SettableGaugeMetric


/**
  * SparkBackend implementation for a Spark Connect session. In Spark Connect mode the client has no access to the
  * SparkContext and the catalog/session state is managed centrally by the server. Consequently, most operations
  * have to be re-implemented on top of the SparkConnect client API. Operations that conceptually require code to
  * be deployed and run inside the connected Spark server (like custom optimizer rules or extensions) are not
  * available on the client and will either be a no-op or implemented via server plugins in a later phase.
  *
  * @param spark
  */
private[execution] final class ConnectBackend(
    override val spark: SparkSession
) extends SparkBackend {
    private final class JobGroups {
        private val group = ThreadLocal.withInitial(() => ("", ""))
        def get : (String,String) = group.get()
        def set(value:(String,String)) : Unit = group.set(value)
    }
    private val jobGroups = new JobGroups
    private val logger = LoggerFactory.getLogger(classOf[ConnectBackend])

    // Spark Connect clients cannot resolve the providing class of a format on the client side. These are the
    // built-in file formats based on FileFormat/FileDataSourceV2 which do support reading multiple paths at once.
    // Any other (custom) format is assumed to support only a single path, which also covers RelationProvider based
    // formats and is the safe default.
    private val multiPathFormats = Seq("parquet", "orc", "json", "csv", "text", "avro", "sequencefile")

    private def unsupported(operation: String) : Nothing = {
        throw new UnsupportedOperationException(s"Feature is not available in Spark Connect mode: $operation")
    }

    override def isConnect: Boolean = true

    override def setJobGroup(jobGroupId: String, jobDescription: String) : Unit = {
        val (prevGroup, _) = getJobGroup
        if (prevGroup != null && prevGroup.nonEmpty && prevGroup != jobGroupId)
            spark.removeTag(prevGroup)
        jobGroups.set((jobGroupId, jobDescription))
        if (jobGroupId != null && jobGroupId.nonEmpty)
            spark.addTag(jobGroupId)
    }

    override def getJobGroup : (String,String) = jobGroups.get

    override def cancelAllJobs() : Unit = spark.interruptAll()

    override def withJobGroup[T](jobGroupId:String, jobDescription:String)(fn: => T) : T = {
        val (prevGroup, prevDescription) = getJobGroup
        setJobGroup(jobGroupId, jobDescription)
        try {
            fn
        }
        finally {
            if (prevGroup != null)
                setJobGroup(prevGroup, prevDescription)
            else {
                jobGroups.set(("", ""))
                if (jobGroupId != null)
                    spark.removeTag(jobGroupId)
            }
        }
    }

    override def hadoopConf : HadoopConf = {
        val sparkConf = new SparkConf(false)
        spark.conf.getAll.foreach { case (key, value) => sparkConf.set(key, value) }
        SparkShim.getHadoopConf(sparkConf)
    }

    override def countRecords(df: org.apache.spark.sql.DataFrame, bundle: MultiMetricBundle, metricName: String, metricLabels: Map[String,String]) : org.apache.spark.sql.DataFrame = {
        // Spark Connect has no client-side mechanism to wrap a DataFrame in a counting plan the way the classic
        // backend does with CountRecordsExec. Using `df.observe(Observation, ...)` is also not viable here, because
        // the Connect client registers observations by the (content-derived) planId, and re-executing the same
        // plan a second time silently keeps the stale observation. We therefore count explicitly and accumulate
        // the result in a SettableGaugeMetric, mirroring the accumulating semantics of the classic LongAccumulator.
        val metric = bundle.getOrCreateMetric(metricName, metricLabels, SettableGaugeMetric(metricName, metricLabels))
        metric.value = metric.value + df.count()
        df
    }

    override def checkpointDir : Option[String] = spark.conf.getOption("spark.checkpoint.dir")

    override def setCheckpointDir(dir : String) : Unit = spark.conf.set("spark.checkpoint.dir", dir)

    override def addJar(jar:String) : Unit = spark.addArtifact(jar)

    override def relationSupportsMultiplePaths(format:String) : Boolean = {
        val normalized = format.toLowerCase(Locale.ROOT)
        multiPathFormats.exists(normalized.contains)
    }

    override def registerStrategies(enableEagerCache:Boolean) : Unit = {
        logger.info("Strategies and optimizer rules must be registered server-side in Spark Connect mode; skipping client-side registration")
    }

    override def registerExtension(config:Configuration) : Unit = {
        logger.info("Spark extensions and UDFs must be registered server-side in Spark Connect mode; skipping client-side registration")
    }
}
