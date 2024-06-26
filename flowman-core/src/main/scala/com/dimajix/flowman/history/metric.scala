/*
 * Copyright 2018-2021 Kaya Kupferschmidt
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

package com.dimajix.flowman.history

import java.sql.Timestamp
import java.time.Clock
import java.time.ZoneId
import java.time.ZonedDateTime

import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.Status
import com.dimajix.flowman.metric.GaugeMetric
import com.dimajix.flowman.metric.Metric


object Measurement {
    def ofMetrics(metrics:Seq[Metric]) : Seq[Measurement] = {
        val now = new Timestamp(Clock.systemDefaultZone().instant().toEpochMilli)
        metrics.flatMap {
            case gauge:GaugeMetric => Some(Measurement(gauge.name, "", now.toInstant.atZone(ZoneId.of("UTC")), gauge.labels, gauge.value))
            case _ => None
        }
    }
}
final case class Measurement(
    name:String,
    jobId:String,
    ts:ZonedDateTime,
    labels:Map[String,String],
    value:Double
)


final case class MetricSeries(
    metric:String,
    namespace:String,
    project:String,
    job:String,
    phase:String,
    labels:Map[String,String],
    measurements:Seq[Measurement]
)
