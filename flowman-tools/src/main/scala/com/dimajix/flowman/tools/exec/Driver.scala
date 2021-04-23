/*
 * Copyright 2018-2019 Kaya Kupferschmidt
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

import scala.util.Failure
import scala.util.Success
import scala.util.Try

import org.apache.hadoop.fs.Path
import org.kohsuke.args4j.CmdLineException
import org.slf4j.LoggerFactory

import com.dimajix.flowman.FLOWMAN_VERSION
import com.dimajix.flowman.JAVA_VERSION
import com.dimajix.flowman.SPARK_VERSION
import com.dimajix.flowman.spec.splitSettings
import com.dimajix.flowman.tools.Logging
import com.dimajix.flowman.tools.Tool
import com.dimajix.flowman.tools.ToolConfig
import com.dimajix.flowman.util.ConsoleColors


object Driver {
    def main(args: Array[String]) : Unit = {
        Logging.init()

        Try {
            run(args:_*)
        }
        match {
            case Success (true) =>
                System.exit(0)
            case Success (false) =>
                System.exit(1)
            case Failure(ex:CmdLineException) =>
                System.err.println(ex.getMessage)
                ex.getParser.printUsage(System.err)
                System.err.println
                System.exit(1)
            case Failure(exception) =>
                exception.printStackTrace(System.err)
                System.exit(1)
        }
    }

    def run(args: String*) : Boolean = {
        val options = new Arguments(args.toArray)
        // Check if only help or version is requested
        if (options.version) {
            println(s"Flowman $FLOWMAN_VERSION")
            println(s"Flowman home: ${ToolConfig.homeDirectory.getOrElse("")}")
            println(s"Spark version $SPARK_VERSION")
            println(s"Java version $JAVA_VERSION")
            true
        }
        else if (options.help) {
            options.printHelp(System.out)
            true
        }
        else {
            Logging.setSparkLogging(options.sparkLogging)

            val driver = new Driver(options)
            driver.run()
        }
    }
}


class Driver(options:Arguments) extends Tool {
    private val logger = LoggerFactory.getLogger(classOf[Driver])

    /**
      * Main method for running this command
      * @return
      */
    def run() : Boolean = {
        // Disable colors in batch mode
        ConsoleColors.disabled = options.batchMode

        val command = options.command
        if (command.help) {
            command.printHelp(System.out)
            true
        }
        else {
            // Create Flowman Session, which also includes a Spark Session
            val project = loadProject(new Path(options.projectFile))
            val config = splitSettings(options.config)
            val environment = splitSettings(options.environment)
            val session = createSession(
                options.sparkMaster,
                options.sparkName,
                project = Some(project),
                additionalConfigs = config.toMap,
                additionalEnvironment = environment.toMap,
                profiles = options.profiles
            )
            val context = session.getContext(project)

            logger.info(s"Flowman $FLOWMAN_VERSION using Spark version $SPARK_VERSION and Java version $JAVA_VERSION")

            val result = options.command.execute(session, project, context)
            session.shutdown()
            result
        }
    }
}
