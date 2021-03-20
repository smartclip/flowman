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

package com.dimajix.flowman.execution

import com.dimajix.common.{No, Trilean, Yes}
import com.dimajix.flowman.model.{BaseTarget, MappingOutputIdentifier, ResourceIdentifier, Target}
import com.fasterxml.jackson.annotation.JsonProperty

case class UncacheTarget(
    instanceProperties: Target.Properties,
    mapping: MappingOutputIdentifier
) extends BaseTarget {
    /**
     * Returns all phases which are implemented by this target in the execute method
     * @return
     */
    override def phases : Set[Phase] = Set(Phase.BUILD)

    /**
      * Returns a list of physical resources required by this target
      * @return
      */
    override def requires(phase: Phase) : Set[ResourceIdentifier] = {
        phase match {
            case Phase.BUILD => MappingUtils.requires(context, mapping.mapping)
            case _ => Set()
        }
    }

    /**
      * Abstract method which will perform the output operation. All required tables need to be
      * registered as temporary tables in the Spark session before calling the execute method.
      *
      * @param execution
      */
    override def build(execution: Execution) : Unit = {
        val mapping = context.getMapping(this.mapping.mapping)
        val df = execution.instantiate(mapping, this.mapping.output)
        df.unpersist()
    }
}

/*
class UncacheTargetSpec extends TargetSpec {
    @JsonProperty(value = "mapping", required=true) private var mapping:String = _

    override def instantiate(context: Context): UncacheTarget = {
        UncacheTarget(
            instanceProperties(context),
            MappingOutputIdentifier.parse(context.evaluate(mapping))
        )
    }
}
*/
