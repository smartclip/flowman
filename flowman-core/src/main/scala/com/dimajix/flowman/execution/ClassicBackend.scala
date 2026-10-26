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

import org.apache.hadoop.conf.{Configuration => HadoopConf}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkShim

import com.dimajix.flowman.config.Configuration
import com.dimajix.flowman.metric.LongAccumulatorMetric
import com.dimajix.flowman.metric.MultiMetricBundle
import com.dimajix.flowman.spi.SparkExtension
import com.dimajix.flowman.spi.UdfProvider
import com.dimajix.spark.SparkUtils
import com.dimajix.spark.sql.catalyst.optimizer.ExtraOptimizations
import com.dimajix.spark.sql.execution.ExtraStrategies
import com.dimajix.spark.sql.functions.count_records


/**
  * SparkBackend implementation for the classic Spark API. All operations delegate to the SparkContext, which is
  * available in this mode.
  */
private[execution] final class ClassicBackend(
    override val spark: SparkSession
) extends SparkBackend {
    private lazy val sc : SparkContext = spark.sparkContext

    override def isConnect: Boolean = false

    override def setJobGroup(jobGroupId: String, jobDescription: String) : Unit = {
        sc.setJobGroup(jobGroupId, jobDescription)
    }

    override def getJobGroup : (String,String) = SparkUtils.getJobGroup(sc)

    override def cancelAllJobs() : Unit = sc.cancelAllJobs()

    override def withJobGroup[T](jobGroupId:String, jobDescription:String)(fn: => T) : T = {
        SparkUtils.withJobGroup(sc, jobGroupId, jobDescription)(fn)
    }

    override def hadoopConf : HadoopConf = sc.hadoopConfiguration

    override def countRecords(df: org.apache.spark.sql.DataFrame, bundle: MultiMetricBundle, metricName: String, metricLabels: Map[String,String]) : org.apache.spark.sql.DataFrame = {
        lazy val newCounter = sc.longAccumulator
        val metric = bundle.getOrCreateMetric(metricName, metricLabels, LongAccumulatorMetric(metricName, metricLabels, newCounter))
        val counter = metric.counter
        count_records(df, counter)
    }

    override def relationSupportsMultiplePaths(format:String) : Boolean =
        SparkShim.relationSupportsMultiplePaths(spark, format)

    override def checkpointDir : Option[String] = sc.getCheckpointDir

    override def setCheckpointDir(dir: String) : Unit = sc.setCheckpointDir(dir)

    override def addJar(jar: String) : Unit = sc.addJar(jar)

    override def registerStrategies(enableEagerCache:Boolean) : Unit = {
        if (enableEagerCache) {
            ExtraOptimizations.enableEagerCache(spark)
        }
        ExtraStrategies.register(spark)
    }

    override def registerExtension(config:Configuration) : Unit = {
        SparkExtension.extensions.foreach(_.register(spark, config))
        UdfProvider.providers.foreach(_.register(spark.udf, config))
    }
}