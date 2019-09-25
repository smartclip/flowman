/*
 * Copyright 2018 Kaya Kupferschmidt
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

package com.dimajix.flowman.spec.target

import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.flowman.annotation.TaskType
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.spec.Module
import com.dimajix.flowman.spec.task.BaseTask
import com.dimajix.flowman.spec.task.Task


case class AnnotatedTarget(instanceProperties:Target.Properties) extends BaseTarget {
}

@TaskType(kind = "annotatedTask")
class AnnotatedTaskSpec extends TargetSpec {
    override def instantiate(context: Context): Target = AnnotatedTarget(instanceProperties(context))
}



class PluginTargetTest extends FlatSpec with Matchers  {
    "A plugin" should "be used if present" in {
        val session = Session.builder().build()
        val spec =
            """
              |jobs:
              |  custom:
              |    tasks:
              |      - kind: annotatedTask
            """.stripMargin
        val module = Module.read.string(spec)
        module.jobs.keys should contain("custom")
        val job = module.jobs("custom").instantiate(session.context)
        job.targets(0) shouldBe an[AnnotatedTaskSpec]
    }

}