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

package com.dimajix.flowman.spec.task

import com.fasterxml.jackson.annotation.JsonProperty
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.RelationIdentifier


object DestroyRelationTask {
    def apply(context: Context, relations:Seq[RelationIdentifier], ignoreIfNotExists:Boolean) : DestroyRelationTask = {
        DestroyRelationTask(
            Task.Properties(context),
            relations,
            ignoreIfNotExists
        )
    }
}

case class DestroyRelationTask(
    instanceProperties:Task.Properties,
    relations:Seq[RelationIdentifier],
    ignoreIfNotExists:Boolean
) extends BaseTask {
    private val logger = LoggerFactory.getLogger(classOf[DestroyRelationTask])

    /**
      * Instantiates all outputs defined in this task
      *
      * @param executor
      * @return
      */
    override def execute(executor:Executor) : Boolean = {
        require(executor != null)

        relations.foreach(o => destroyRelation(executor, o))
        true
    }

    private def destroyRelation(executor: Executor, relationName: RelationIdentifier) : Boolean = {
        require(executor != null)
        require(relationName != null)

        logger.info(s"Destroying relation '$relationName'")
        val relation = context.getRelation(relationName)
        relation.destroy(executor, ignoreIfNotExists)
        true
    }
}



class DestroyRelationTaskSpec extends TaskSpec {
    @JsonProperty(value = "relation", required = true) private var relations: Seq[String] = Seq()
    @JsonProperty(value = "ignoreIfNotExists", required = false) private var ignoreIfNotExists: String = "false"

    override def instantiate(context: Context): Task = {
        DestroyRelationTask(
            instanceProperties(context),
            relations.map(i => RelationIdentifier(context.evaluate(i))),
            context.evaluate(ignoreIfNotExists).toBoolean
        )
    }
}
