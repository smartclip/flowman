/*
 * Copyright 2019-2022 Kaya Kupferschmidt
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

package com.dimajix.flowman.spec.target

import com.fasterxml.jackson.annotation.JsonProperty

import com.dimajix.common.Trilean
import com.dimajix.flowman.common.ParserUtils.splitSettings
import com.dimajix.flowman.documentation.TargetDoc
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.ScopeContext
import com.dimajix.flowman.graph.Linker
import com.dimajix.flowman.model.BaseTarget
import com.dimajix.flowman.model.Metadata
import com.dimajix.flowman.model.ResourceIdentifier
import com.dimajix.flowman.model.Target
import com.dimajix.flowman.model.TargetDigest
import com.dimajix.flowman.model.TargetIdentifier
import com.dimajix.flowman.model.TargetResult


case class TemplateTarget(
    override val instanceProperties:Target.Properties,
    target:TargetIdentifier,
    environment:Map[String,String]
) extends BaseTarget {
    private val templateContext = ScopeContext.builder(context)
        .withEnvironment(environment)
        .build()
    lazy val targetInstance : Target = {
        val meta = Metadata(templateContext, name, category, "")
        val props = Target.Properties(templateContext, meta, super.before, super.after, super.description, super.documentation)
        val spec = project.get.targets(target.name)
        spec.instantiate(templateContext, Some(props))
    }

    /**
     * Returns a description of the build target
     *
     * @return
     */
    override def description: Option[String] = instanceProperties.description.orElse(targetInstance.description)

    /**
     * Returns a (static) documentation of this target
 *
     * @return
     */
    override def documentation : Option[TargetDoc] = {
        targetInstance.documentation
            .map(_.copy(target=Some(this)))
    }

    /**
     * Returns an instance representing this target with the context
     * @return
     */
    override def digest(phase:Phase) : TargetDigest = {
        val parent = targetInstance.digest(phase)
        TargetDigest(
            namespace.map(_.name).getOrElse(""),
            project.map(_.name).getOrElse(""),
            name,
            phase,
            parent.partitions
        )
    }

    /**
     * Returns all phases which are implemented by this target in the execute method
     * @return
     */
    override def phases : Set[Phase] = targetInstance.phases

    /**
     * Returns an explicit user defined list of targets to be executed after this target. I.e. this
     * target needs to be executed before all other targets in this list.
     *
     * @return
     */
    override def before: Seq[TargetIdentifier] = targetInstance.before

    /**
     * Returns an explicit user defined list of targets to be executed before this target I.e. this
     * * target needs to be executed after all other targets in this list.
     *
     * @return
     */
    override def after: Seq[TargetIdentifier] = targetInstance.after

    /**
     * Returns a list of physical resources produced by this target
     *
     * @return
     */
    override def provides(phase: Phase): Set[ResourceIdentifier] = targetInstance.provides(phase)

    /**
     * Returns a list of physical resources required by this target
     *
     * @return
     */
    override def requires(phase: Phase): Set[ResourceIdentifier] = targetInstance.requires(phase)

    /**
     * Returns the state of the target, specifically of any artifacts produces. If this method return [[Yes]],
     * then an [[execute]] should update the output, such that the target is not 'dirty' any more.
     *
     * @param execution
     * @param phase
     * @return
     */
    override def dirty(execution: Execution, phase: Phase): Trilean = targetInstance.dirty(execution, phase)

    /**
     * Executes a specific phase of this target. This method is should not throw a non-fatal exception, instead it
     * should wrap any exception in the TargetResult
     *
     * @param execution
     * @param phase
     */
    override def execute(execution: Execution, phase: Phase): TargetResult = {
        val result = targetInstance.execute(execution, phase)
        result.copy(target=this, instance=digest(phase))
    }

    /**
     * Creates all known links for building a descriptive graph of the whole data flow
     * Params: linker - The linker object to use for creating new edges
     */
    override def link(linker: Linker, phase:Phase): Unit = {
        targetInstance.link(linker, phase)
    }
}


class TemplateTargetSpec extends TargetSpec {
    @JsonProperty(value = "target", required = true) private var target:String = _
    @JsonProperty(value = "environment", required = true) private var environment:Seq[String] = Seq()

    /**
     * Creates an instance of this specification and performs the interpolation of all variables
     *
     * @param context
     * @return
     */
    override def instantiate(context: Context, properties:Option[Target.Properties] = None): TemplateTarget = {
        TemplateTarget(
            instanceProperties(context, properties),
            TargetIdentifier(context.evaluate(target)),
            splitSettings(environment).toMap
        )
    }
}
