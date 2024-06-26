/*
 * Copyright 2022 Kaya Kupferschmidt
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

package com.dimajix.flowman.spi

import java.util.ServiceLoader

import scala.collection.JavaConverters._

import org.apache.spark.sql.DataFrame

import com.dimajix.flowman.documentation.SchemaCheck
import com.dimajix.flowman.documentation.CheckResult
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution


object SchemaCheckExecutor {
    def executors : Seq[SchemaCheckExecutor] = {
        val loader = ServiceLoader.load(classOf[SchemaCheckExecutor])
        loader.iterator().asScala.toSeq
    }
}

trait SchemaCheckExecutor {
    def execute(execution: Execution, context:Context, df:DataFrame, test:SchemaCheck) : Option[CheckResult]
}
