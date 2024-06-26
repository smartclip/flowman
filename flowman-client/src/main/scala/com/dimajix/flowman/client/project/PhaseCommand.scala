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

package com.dimajix.flowman.client.project

import java.net.URI

import org.apache.http.impl.client.CloseableHttpClient
import org.kohsuke.args4j.Argument
import org.kohsuke.args4j.Option
import org.slf4j.LoggerFactory

import com.dimajix.flowman.client.Command
import com.dimajix.flowman.common.ParserUtils.splitSettings


sealed class PhaseCommand(phase:String) extends Command {
    private val logger = LoggerFactory.getLogger(getClass)

    @Argument(index=0, required=true, usage = "specifies project to run", metaVar = "<project>")
    var job: String = ""
    @Argument(index=1, required=false, usage = "specifies project main jobs parameters", metaVar = "<param>=<value>")
    var args: Array[String] = Array()
    @Option(name = "-t", aliases=Array("--target"), usage = "only process specific targets, as specified by a regex", metaVar = "<target>")
    var targets: Array[String] = Array(".*")
    @Option(name = "-d", aliases=Array("--dirty"), usage = "mark targets as being dirty, as specified by a regex", metaVar = "<target>")
    var dirtyTargets: Array[String] = Array()
    @Option(name = "-f", aliases=Array("--force"), usage = "forces execution, even if outputs are already created")
    var force: Boolean = false
    @Option(name = "-k", aliases=Array("--keep-going"), usage = "continues execution of job with next target in case of errors")
    var keepGoing: Boolean = false
    @Option(name = "--dry-run", usage = "perform dry run without actually executing build targets")
    var dryRun: Boolean = false
    @Option(name = "-nl", aliases=Array("--no-lifecycle"), usage = "only executes the specific phase and not the whole lifecycle")
    var noLifecycle: Boolean = false

    override def execute(httpClient:CloseableHttpClient, baseUri:URI) : Boolean = {
        val args = splitSettings(this.args).toMap
        ???
    }
}

class ValidateCommand extends PhaseCommand("VALIDATE")
class CreateCommand extends PhaseCommand("CREATE")
class BuildCommand extends PhaseCommand("BUILD")
class VerifyCommand extends PhaseCommand("VERIFY")
class TruncateCommand extends PhaseCommand("TRUNCATE")
class DestroyCommand extends PhaseCommand("DESTROY")
