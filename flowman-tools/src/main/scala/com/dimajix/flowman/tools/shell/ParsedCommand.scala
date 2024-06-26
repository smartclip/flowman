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

package com.dimajix.flowman.tools.shell

import org.kohsuke.args4j.Argument
import org.kohsuke.args4j.spi.SubCommand
import org.kohsuke.args4j.spi.SubCommandHandler
import org.kohsuke.args4j.spi.SubCommands

import com.dimajix.flowman.tools.exec.Command
import com.dimajix.flowman.tools.exec.VersionCommand
import com.dimajix.flowman.tools.exec.documentation.DocumentationCommand
import com.dimajix.flowman.tools.exec.info.InfoCommand
import com.dimajix.flowman.tools.exec.mapping.MappingCommand
import com.dimajix.flowman.tools.exec.relation.ModelCommand
import com.dimajix.flowman.tools.exec.namespace.NamespaceCommand
import com.dimajix.flowman.tools.exec.sql.SqlCommand
import com.dimajix.flowman.tools.exec.target.TargetCommand
import com.dimajix.flowman.tools.shell.job.JobCommand
import com.dimajix.flowman.tools.shell.project.ProjectCommand
import com.dimajix.flowman.tools.shell.test.TestCommand
import com.dimajix.flowman.tools.exec.history.HistoryCommand


class ParsedCommand {
    @Argument(required=false,index=0,metaVar="<command-group>",usage="the object to work with",handler=classOf[SubCommandHandler])
    @SubCommands(Array(
        new SubCommand(name="documentation",impl=classOf[DocumentationCommand]),
        new SubCommand(name="eval",impl=classOf[EvaluateCommand]),
        new SubCommand(name="exit",impl=classOf[ExitCommand]),
        new SubCommand(name="history",impl=classOf[HistoryCommand]),
        new SubCommand(name="info",impl=classOf[InfoCommand]),
        new SubCommand(name="job",impl=classOf[JobCommand]),
        new SubCommand(name="mapping",impl=classOf[MappingCommand]),
        new SubCommand(name="model",impl=classOf[ModelCommand]),
        new SubCommand(name="namespace",impl=classOf[NamespaceCommand]),
        new SubCommand(name="project",impl=classOf[ProjectCommand]),
        new SubCommand(name="quit",impl=classOf[ExitCommand]),
        new SubCommand(name="relation",impl=classOf[ModelCommand]),
        new SubCommand(name="sql",impl=classOf[SqlCommand]),
        new SubCommand(name="target",impl=classOf[TargetCommand]),
        new SubCommand(name="test",impl=classOf[TestCommand]),
        new SubCommand(name="version",impl=classOf[VersionCommand])
    ))
    var command:Command = _

}
