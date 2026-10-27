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

import org.apache.spark.sql.SparkSessionExtensions

import com.dimajix.spark.sql.catalyst.optimizer.CreateEagerCache
import com.dimajix.spark.sql.execution.ExtraStrategies


/**
  * SparkSessionExtensions which recreate the classic Flowman optimizer/planner behavior on a Spark Connect server.
  *
  * Register this class as the value of the `spark.sql.extensions` config on the Spark Connect server (e.g. when
  * installing the Flowman server plugin):
  *
  * {{{
  *   spark.sql.extensions=com.dimajix.flowman.connect.server.FlowmanSparkSessionExtensions
  * }}}
  *
  * The registered extra strategies are required to plan the EagerCache and CountRecords logical nodes (introduced by
  * the Flowman optimizer rule and the Flowman relation plugin) into physical plans, and the eager-cache optimizer
  * rule wraps repeated cache usages in an EagerCache node so that a cached relation is only computed once.
  */
class FlowmanSparkSessionExtensions extends (SparkSessionExtensions => Unit) {
    override def apply(extensions:SparkSessionExtensions) : Unit = {
        extensions.injectPlannerStrategy(_ => ExtraStrategies)
        extensions.injectOptimizerRule(_ => CreateEagerCache)
    }
}
