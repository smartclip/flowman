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

package com.dimajix.flowman.model

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.model


object Connection {
    object Properties {
        def apply(context: Context, name:String = "", kind:String = "") : Properties = {
            Properties(
                context,
                Metadata(context, name, Category.CONNECTION, kind)
            )
        }
    }
    final case class Properties(
        context: Context,
        metadata:Metadata
    ) extends model.Properties[Properties] {
        require(metadata.category == Category.CONNECTION.lower)
        require(metadata.namespace == context.namespace.map(_.name))
        require(metadata.project == context.project.map(_.name))
        require(metadata.version == context.project.flatMap(_.version))

        override val namespace:Option[Namespace] = context.namespace
        override val project:Option[Project] = context.project
        override val kind : String = metadata.kind
        override val name : String = metadata.name

        override def withName(name: String): Properties = copy(metadata=metadata.copy(name = name))

        def merge(other: Properties): Properties = {
            Properties(context, metadata.merge(other.metadata))
        }
    }
}

/**
 * Base class to be used for all Connection instances
 */
trait Connection extends Instance {
    override type PropertiesType = Connection.Properties

    /**
     * Returns the category of this resource
     *
     * @return
     */
    final override def category: Category = Category.CONNECTION

    /**
     * Returns an identifier for this connection
     *
     * @return
     */
    def identifier: ConnectionIdentifier
}


abstract class BaseConnection extends AbstractInstance with Connection {
    protected override def instanceProperties : Connection.Properties

    /**
     * Returns an identifier for this connection
     * @return
     */
    def identifier : ConnectionIdentifier = ConnectionIdentifier(name, project.map(_.name))
}
