/*
 * Copyright 2021 Kaya Kupferschmidt
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

package com.dimajix.flowman.model

import scala.util.control.NonFatal

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.ScopeContext
import com.dimajix.flowman.types.FieldType


object Template {
    object Properties {
        def apply(context: Context, name:String = "") : Properties = {
            Properties(
                context,
                context.namespace,
                context.project,
                name,
                "",
                Map()
            )
        }
    }
    final case class Properties(
        context:Context,
        namespace:Option[Namespace],
        project:Option[Project],
        name:String,
        kind:String,
        labels:Map[String,String]
    ) extends Instance.Properties[Properties] {
        override def withName(name: String): Properties = copy(name=name)
        def identifier : TemplateIdentifier = TemplateIdentifier(name, project.map(_.name))
    }


    final case class Parameter(
        name:String,
        ftype : FieldType,
        default: Option[Any] = None,
        description: Option[String]=None
    ) {
        /**
         * Pasres a string representing a single value for the parameter
         * @param value
         * @return
         */
        def parse(value:String) : Any = {
            ftype.parse(value)
        }
    }
}


trait Template[T] extends Instance {
    /**
     * Returns the category of this resource
     * @return
     */
    final override def category: Category = Category.TEMPLATE

    /**
     * Returns an identifier for this target
     * @return
     */
    def identifier : TemplateIdentifier

    /**
     * Returns the list of parameters required for instantiation of this template
     */
    def parameters : Seq[Template.Parameter]

    /**
     * Instantiate this template with the given parameters
     * @param context
     * @param name
     * @param args
     * @return
     */
    def instantiate(context: Context, name:String, args:Map[String,Any]): T

    /**
     * Determine final arguments of this job, by performing granularity adjustments etc. Missing arguments will
     * be replaced by default values if they are defined.
     * @param args
     * @return
     */
    def arguments(args:Map[String,String]) : Map[String,Any] = {
        val paramsByName = parameters.map(p => (p.name, p)).toMap
        val processedArgs = args.map { case (pname, sval) =>
            val param = paramsByName.getOrElse(pname, throw new IllegalArgumentException(s"Parameter '$pname' not defined for template '$name'"))
            val pval = try {
                param.parse(sval)
            }
            catch {
                case NonFatal(ex) => throw new IllegalArgumentException(s"Cannot parse parameter '$pname' of template '$name' with value '$sval'", ex)
            }
            (pname, pval)
        }
        parameters.map { p =>
            val pname = p.name
            pname -> processedArgs.get(pname)
                .orElse(p.default)
                .getOrElse(throw new IllegalArgumentException(s"Missing parameter '$pname' in template '$name'"))
        }.toMap
    }
}


abstract class BaseTemplate[T] extends AbstractInstance with Template[T] {
    protected override def instanceProperties : Template.Properties

    /**
     * Returns an identifier for this target
     * @return
     */
    override def identifier : TemplateIdentifier = instanceProperties.identifier

    /**
     * Instantiate this template with the given parameters
     * @param context
     * @param name
     * @param args
     * @return
     */
    def instantiate(context: Context, name:String, args:Map[String,Any]): T = {
        // Validate args!
        val ctxt = ScopeContext.builder(context)
            .withEnvironment(args)
            .build()

        instantiateInternal(ctxt, name)
    }

    protected def instantiateInternal(context: Context, name:String) : T
}


trait RelationTemplate extends Template[Relation]
trait MappingTemplate extends Template[Mapping]
trait TargetTemplate extends Template[Target]
trait AssertionTemplate extends Template[Assertion]
trait DatasetTemplate extends Template[Dataset]
trait SchemaTemplate extends Template[Schema]
trait MeasureTemplate extends Template[Measure]
trait ConnectionTemplate extends Template[Connection]
