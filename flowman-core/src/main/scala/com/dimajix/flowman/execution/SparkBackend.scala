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
import org.apache.spark.sql.SparkSession

import com.dimajix.flowman.config.Configuration
import com.dimajix.flowman.metric.MultiMetricBundle
import com.dimajix.flowman.metric.Metric


/**
  * Abstraction over the two Spark execution modes (classic and Connect). Flowman code should use this interface
  * instead of reaching directly into the SparkContext or SessionState, both of which are not available on a Spark
  * Connect endpoint. The concrete implementation is selected by [[SparkBackend.of]] based on the actual
  * [[SparkSession]] implementation.
  */
trait SparkBackend {
    /**
      * Returns the underlying SparkSession
      */
    def spark: SparkSession

    /**
      * Returns true if this backend wraps a Spark Connect session, i.e. [[spark]] does not give access to the
      * SparkContext and SessionState.
      */
    def isConnect: Boolean

    /**
      * Sets a new Spark job group for the current thread.
      */
    def setJobGroup(jobGroupId: String, jobDescription: String): Unit

    /**
      * Returns the current Spark job group (groupId, description).
      */
    def getJobGroup: (String, String)

    /**
      * Cancels all currently running jobs
      */
    def cancelAllJobs(): Unit

    /**
      * Executes a block in a new Spark job group, restoring the previous job group afterwards.
      */
    def withJobGroup[T](jobGroupId: String, jobDescription: String)(fn: => T): T

    /**
      * Returns the Hadoop configuration as used by Spark.
      */
    def hadoopConf: HadoopConf

    /**
      * Wraps the given DataFrame such that the number of records is counted during execution, and registers a
      * corresponding GaugeMetric within the given MetricBundle. The returned DataFrame must be used for the actual
      * write / execution step, only then the metric will be populated.
      *
      * @param df       - the DataFrame to wrap
      * @param bundle   - the MetricBundle to register the metric with
      * @param metricName - the name of the metric
      * @param metricLabels - the labels of the metric
      * @return the wrapped DataFrame
      */
    def countRecords(df: org.apache.spark.sql.DataFrame, bundle: MultiMetricBundle, metricName: String, metricLabels: Map[String,String]): org.apache.spark.sql.DataFrame

    /**
      * Returns true if the given file format is able to read several (possibly unrelated) locations at once.
      * This is used to decide between `DataFrameReader.load(path*)` and `DataFrameReader.load(path1, path2, ...)`.
      */
    def relationSupportsMultiplePaths(format:String) : Boolean

    /**
      * Returns the configured checkpoint directory, if any.
      */
    def checkpointDir: Option[String]

    /**
      * Sets the checkpoint directory.
      */
    def setCheckpointDir(dir: String): Unit

    /**
      * Distributes an additional jar file to all workers.
      */
    def addJar(jar: String): Unit

    /**
      * Registers additional planner strategies and optimizer rules (e.g. EagerCache, CountRecords).
      */
    def registerStrategies(enableEagerCache: Boolean): Unit

    /**
      * Registers the session extensions and UDFs provided via the ServiceLoader mechanism.
      */
    def registerExtension(config: Configuration): Unit
}


object SparkBackend {
    def of(spark:SparkSession) : SparkBackend = {
        if (spark.getClass.getName == "org.apache.spark.sql.connect.SparkSession")
            new ConnectBackend(spark)
        else
            new ClassicBackend(spark)
    }
}
