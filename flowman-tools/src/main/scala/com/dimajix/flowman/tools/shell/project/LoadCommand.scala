/*
 * Copyright 2018-2022 Kaya Kupferschmidt
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

package com.dimajix.flowman.tools.shell.project

import scala.util.control.NonFatal

import org.apache.hadoop.fs.Path
import org.kohsuke.args4j.Argument
import org.slf4j.LoggerFactory

import com.dimajix.common.ExceptionUtils.reasons
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.execution.Status
import com.dimajix.flowman.model.Project
import com.dimajix.flowman.tools.exec.Command
import com.dimajix.flowman.tools.shell.Shell


class LoadCommand extends Command {
    private val logger = LoggerFactory.getLogger(classOf[LoadCommand])

    @Argument(index=0, required=true, usage = "filename or directory of project to load", metaVar = "<filename>")
    var project: String = ""

    override def execute(session: Session, project:Project, context:Context): Status = {
        try {
            Shell.instance.loadProject(new Path(this.project))
            Status.SUCCESS
        }
        catch {
            case NonFatal(e) =>
                logger.error(s"Error loading project '${this.project}':\n  ${reasons(e)}")
                Status.FAILED
        }
    }
}
