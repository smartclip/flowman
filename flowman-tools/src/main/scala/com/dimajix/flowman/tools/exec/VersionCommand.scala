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

package com.dimajix.flowman.tools.exec

import com.dimajix.flowman.FLOWMAN_VERSION
import com.dimajix.flowman.JAVA_VERSION
import com.dimajix.flowman.SPARK_VERSION
import com.dimajix.flowman.common.ToolConfig
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.model.Project


class VersionCommand extends Command {
    override def execute(session: Session, project: Project, context: Context): Boolean = {
        println(s"Flowman $FLOWMAN_VERSION")
        println(s"Flowman home: ${ToolConfig.homeDirectory.getOrElse("")}")
        println(s"Spark version $SPARK_VERSION")
        println(s"Java version $JAVA_VERSION")
        true
    }
}
