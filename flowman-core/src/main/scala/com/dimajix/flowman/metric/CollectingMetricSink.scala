/*
 * Copyright 2021 Kaya Kupferschmidt
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

import java.util.concurrent.atomic.AtomicReference

import com.dimajix.flowman.execution.Status


class CollectingMetricSink extends AbstractMetricSink {
    private val currentMetrics = new AtomicReference[Seq[Metric]](Seq())

    def metrics = currentMetrics.get()

    override def commit(board:MetricBoard, status:Status): Unit = {
        currentMetrics.set(board.metrics(catalog(board), status))
    }
}
