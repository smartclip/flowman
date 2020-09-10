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

package com.dimajix.flowman.tools.shell.job

import org.kohsuke.args4j.Argument
import org.kohsuke.args4j.spi.SubCommand
import org.kohsuke.args4j.spi.SubCommandHandler
import org.kohsuke.args4j.spi.SubCommands

import com.dimajix.flowman.tools.exec.Command
import com.dimajix.flowman.tools.exec.NestedCommand
import com.dimajix.flowman.tools.exec.job.BuildCommand
import com.dimajix.flowman.tools.exec.job.CreateCommand
import com.dimajix.flowman.tools.exec.job.DestroyCommand
import com.dimajix.flowman.tools.exec.job.ListCommand
import com.dimajix.flowman.tools.exec.job.TruncateCommand
import com.dimajix.flowman.tools.exec.job.VerifyCommand


class JobCommand extends NestedCommand {
    @Argument(required=true,index=0,metaVar="<subcommand>",usage="the subcommand to run",handler=classOf[SubCommandHandler])
    @SubCommands(Array(
        new SubCommand(name="list",impl=classOf[ListCommand]),
        new SubCommand(name="create",impl=classOf[CreateCommand]),
        new SubCommand(name="build",impl=classOf[BuildCommand]),
        new SubCommand(name="verify",impl=classOf[VerifyCommand]),
        new SubCommand(name="truncate",impl=classOf[TruncateCommand]),
        new SubCommand(name="destroy",impl=classOf[DestroyCommand]),
        new SubCommand(name="info",impl=classOf[InfoCommand]),
        new SubCommand(name="enter",impl=classOf[EnterCommand]),
        new SubCommand(name="leave",impl=classOf[LeaveCommand])
    ))
    override var command:Command = _
}