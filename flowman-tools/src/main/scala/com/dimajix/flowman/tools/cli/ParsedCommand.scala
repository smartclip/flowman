/*
 * Copyright 2020 Kaya Kupferschmidt
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

package com.dimajix.flowman.tools.cli

import org.kohsuke.args4j.Argument
import org.kohsuke.args4j.spi.SubCommand
import org.kohsuke.args4j.spi.SubCommandHandler
import org.kohsuke.args4j.spi.SubCommands

import com.dimajix.flowman.tools.exec.Command
import com.dimajix.flowman.tools.exec.info.InfoCommand
import com.dimajix.flowman.tools.exec.job.JobCommand
import com.dimajix.flowman.tools.exec.mapping.MappingCommand
import com.dimajix.flowman.tools.exec.model.ModelCommand
import com.dimajix.flowman.tools.exec.project.ProjectCommand
import com.dimajix.flowman.tools.exec.target.TargetCommand


class ParsedCommand {
    @Argument(required=false,index=0,metaVar="<command-group>",usage="the object to work with",handler=classOf[SubCommandHandler])
    @SubCommands(Array(
        new SubCommand(name="info",impl=classOf[InfoCommand]),
        new SubCommand(name="job",impl=classOf[JobCommand]),
        new SubCommand(name="model",impl=classOf[ModelCommand]),
        new SubCommand(name="relation",impl=classOf[ModelCommand]),
        new SubCommand(name="mapping",impl=classOf[MappingCommand]),
        new SubCommand(name="target",impl=classOf[TargetCommand]),
        new SubCommand(name="project",impl=classOf[ProjectCommand])
    ))
    var command:Command = _

}