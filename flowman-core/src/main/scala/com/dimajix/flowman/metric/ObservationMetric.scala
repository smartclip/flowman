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

package com.dimajix.flowman.metric

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt
import scala.util.Try

import org.apache.spark.sql.Observation


/**
  * A [[GaugeMetric]] which reads its value from a Spark [[Observation]]. This is the Spark Connect equivalent
  * of the [[LongAccumulatorMetric]]: the value is only available after the observed DataFrame has been
  * executed. Before that, the value reads as `0.0`.
  *
  * A Spark [[Observation]] can only be used with a single Dataset, therefore the current [[observation]]
  * is re-bound on every call to [[com.dimajix.flowman.execution.SparkBackend.countRecords]]. Since a fresh
  * Observation only reports the records observed by the most recent execution, the values of superseded
  * Observations are folded into [[base]] in order to mirror the accumulating semantics of the classic
  * [[LongAccumulator]] based implementation (where the `target_records` gauge counts all records processed
  * since the last [[reset]]).
  */
final case class ObservationMetric(
    override val name: String,
    override val labels: Map[String, String],
    var observation: Observation,
    val metricName: String
) extends GaugeMetric {
    private var base = 0.0
    private var ignoreObservation = false

    /**
      * Re-binds this metric to a freshly created Observation which is used for a subsequent execution of an
      * observed DataFrame. The value of the superseded Observation is folded into [[base]], thereby accumulating
      * record counts over multiple executions.
      */
    def setObservation(obs: Observation): Unit = synchronized {
        if (!ignoreObservation)
            base += currentValue
        observation = obs
        ignoreObservation = false
    }

    override def value: Double = synchronized {
        base + currentValue
    }

    override def reset(): Unit = synchronized {
        base = 0.0
        ignoreObservation = true
    }

    private def currentValue: Double = {
        if (ignoreObservation)
            0.0
        else {
            val metrics = Try {
                val row = Await.result(observation.future, 100.millis)
                if (row == null || row.schema == null)
                    Map.empty[String, Any]
                else
                    row.getValuesMap(row.schema.map(_.name))
            }.getOrElse(Map.empty[String, Any])
            metrics.get(metricName).flatMap(v => Try(v.toString.toDouble).toOption).getOrElse(0.0)
        }
    }
}
