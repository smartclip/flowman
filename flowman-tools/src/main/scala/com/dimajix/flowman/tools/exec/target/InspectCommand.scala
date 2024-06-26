/*
 * Copyright 2021-2022 Kaya Kupferschmidt
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

package com.dimajix.flowman.tools.exec.target

import scala.util.control.NonFatal

import org.kohsuke.args4j.Argument
import org.slf4j.LoggerFactory

import com.dimajix.common.ExceptionUtils.reasons
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Lifecycle
import com.dimajix.flowman.execution.NoSuchTargetException
import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.execution.Status
import com.dimajix.flowman.model.Project
import com.dimajix.flowman.model.Target
import com.dimajix.flowman.model.TargetIdentifier
import com.dimajix.flowman.tools.exec.Command


class InspectCommand extends Command {
    private val logger = LoggerFactory.getLogger(classOf[InspectCommand])

    @Argument(required = true, usage = "specifies target to inspect", metaVar = "<target>")
    var target: String = ""

    override def execute(session: Session, project: Project, context: Context): Status = {
        try {
            val target = context.getTarget(TargetIdentifier(this.target))
            println("Target:")
            println(s"    name: ${target.name}")
            println(s"    kind: ${target.kind}")
            println(s"    phases: ${target.phases.mkString(",")}")
            println(s"    before: ${target.before.mkString(",")}")
            println(s"    after: ${target.after.mkString(",")}")
            Lifecycle.ALL.foreach(p => printDependencies(target,p))
            Status.SUCCESS
        }
        catch {
            case ex:NoSuchTargetException =>
                logger.error(s"Cannot resolve target '${ex.target}'")
                Status.FAILED
            case NonFatal(e) =>
                logger.error(s"Error inspecting '$target':\n  ${reasons(e)}")
                Status.FAILED
        }
    }

    private def printDependencies(target:Target, phase:Phase) : Unit = {
        println(s"Phase '$phase' ${if (!target.phases.contains(phase)) " (inactive)" else ""}:")
        println(s"  Provides:")
        target.provides(phase)
            .map(_.text)
            .toSeq.sorted
            .foreach{ p => println(s"    $p") }
        println(s"  Requires:")
        target.requires(phase)
            .map(_.text)
            .toSeq.sorted
            .foreach{ p => println(s"    $p") }
    }
}
