/*
 * Copyright 2020-2022 Kaya Kupferschmidt
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

package com.dimajix.flowman.tools.exec.history

import org.kohsuke.args4j.Option

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.execution.Status
import com.dimajix.flowman.history.JobOrder
import com.dimajix.flowman.history.JobQuery
import com.dimajix.flowman.model.Project
import com.dimajix.flowman.common.ParserUtils.splitSettings
import com.dimajix.flowman.tools.exec.Command
import com.dimajix.flowman.common.ConsoleUtils


class SearchJobHistoryCommand extends Command {
    @Option(name = "-P", aliases=Array("--project"), usage = "name of project", metaVar = "<project>")
    var project:String = ""
    @Option(name = "-j", aliases=Array("--job"), usage = "name of job", metaVar = "<job>")
    var job:String = ""
    @Option(name = "-s", aliases=Array("--status"), usage = "status of job (UNKNOWN, RUNNING, SUCCESS, FAILED, ABORTED, SKIPPED)", metaVar = "<status>")
    var status:String = ""
    @Option(name = "-p", aliases=Array("--phase"), usage = "execution phase (CREATE, BUILD, VERIFY, TRUNCATE, DESTROY)", metaVar = "<phase>")
    var phase:String = ""
    @Option(name = "-a", aliases=Array("--arg"), usage = "job argument (key=value)", metaVar = "<phase>")
    var args:Array[String] = Array()
    @Option(name = "-n", aliases=Array("--limit"), usage = "maximum number of results", metaVar = "<limit>")
    var limit:Int = 100

    override def execute(session: Session, project: Project, context: Context): Status = {
        val query = JobQuery(
            namespace = session.namespace.map(_.name).toSeq,
            project = split(Some(this.project).filter(_.nonEmpty).getOrElse(project.name)),
            job = split(job),
            status = split(status).map(Status.ofString),
            phase = split(phase).map(Phase.ofString),
            args = splitSettings(args).toMap
        )
        val jobs = session.history.findJobs(query, Seq(JobOrder.BY_DATETIME), limit, 0)
        ConsoleUtils.showTable(jobs)
        Status.SUCCESS
    }

    private def split(arg:String) : Seq[String] = {
        arg.split(',').map(_.trim).filter(_.nonEmpty)
    }
}
