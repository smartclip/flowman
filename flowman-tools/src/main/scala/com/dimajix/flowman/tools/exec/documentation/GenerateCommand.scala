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

package com.dimajix.flowman.tools.exec.documentation

import scala.util.Failure
import scala.util.Success
import scala.util.Try
import scala.util.control.NonFatal

import org.apache.hadoop.fs.Path
import org.kohsuke.args4j.Argument
import org.slf4j.LoggerFactory

import com.dimajix.common.ExceptionUtils.reasons
import com.dimajix.flowman.common.ParserUtils.splitSettings
import com.dimajix.flowman.documentation.Documenter
import com.dimajix.flowman.documentation.MappingCollector
import com.dimajix.flowman.documentation.RelationCollector
import com.dimajix.flowman.documentation.TargetCollector
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.execution.Status
import com.dimajix.flowman.model.Job
import com.dimajix.flowman.model.JobIdentifier
import com.dimajix.flowman.model.Project
import com.dimajix.flowman.spec.documentation.FileGenerator
import com.dimajix.flowman.tools.exec.Command
import com.dimajix.flowman.types.FieldValue


class GenerateCommand extends Command {
    private val logger = LoggerFactory.getLogger(getClass)

    @Argument(index=0, required=false, usage = "specifies job to document", metaVar = "<job>")
    var job: String = "main"
    @Argument(index=1, required=false, usage = "specifies job parameters", metaVar = "<param>=<value>")
    var args: Array[String] = Array()

    override def execute(session: Session, project: Project, context:Context) : Status = {
        val args = splitSettings(this.args).toMap
        Try {
            context.getJob(JobIdentifier(job))
        }
        match {
            case Failure(e) =>
                logger.error(s"Error instantiating job '$job': ${reasons(e)}")
                Status.FAILED
            case Success(job) =>
                generateDoc(session, job, job.arguments(args))
        }
    }

    private def generateDoc(session: Session, job:Job, args:Map[String,Any]) : Status = {
        val collectors = Seq(
            new RelationCollector(),
            new MappingCollector(),
            new TargetCollector()
        )
        val generators = Seq(
            new FileGenerator(new Path("/tmp/flowman/doc"))
        )
        val documenter = Documenter(
            collectors,
            generators
        )

        try {
            documenter.execute(session, job, args)
            Status.SUCCESS
        } catch {
            case NonFatal(ex) =>
                logger.error("Cannot generate documentation: " + reasons(ex))
                Status.FAILED
        }
    }
}