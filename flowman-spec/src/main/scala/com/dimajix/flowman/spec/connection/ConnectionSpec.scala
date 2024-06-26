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

package com.dimajix.flowman.spec.connection

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonProperty.Access
import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.databind.annotation.JsonTypeResolver

import com.dimajix.common.TypeRegistry
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.model.Category
import com.dimajix.flowman.model.Connection
import com.dimajix.flowman.model.Metadata
import com.dimajix.flowman.spec.NamedSpec
import com.dimajix.flowman.spec.annotation.ConnectionType
import com.dimajix.flowman.spec.template.CustomTypeResolverBuilder
import com.dimajix.flowman.spi.ClassAnnotationHandler


object ConnectionSpec extends TypeRegistry[ConnectionSpec] {
    final class NameResolver extends NamedSpec.NameResolver[ConnectionSpec]
}

/**
 * The ConnectionSpec class contains the raw specification values, which may require interpolation.
 */
@JsonTypeResolver(classOf[CustomTypeResolverBuilder])
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "kind", defaultImpl = classOf[JdbcConnectionSpec], visible = true)
@JsonSubTypes(value = Array(
    new JsonSubTypes.Type(name = "jdbc", value = classOf[JdbcConnectionSpec]),
    new JsonSubTypes.Type(name = "ssh", value = classOf[SshConnectionSpec]),
    new JsonSubTypes.Type(name = "sftp", value = classOf[SshConnectionSpec])
))
abstract class ConnectionSpec extends NamedSpec[Connection] {
    @JsonProperty(value="kind", access=Access.WRITE_ONLY, required = true) protected var kind: String = "jdbc"

    /**
     * Creates an instance of this specification and performs the interpolation of all variables
     *
     * @param context
     * @return
     */
    override def instantiate(context: Context, properties:Option[Connection.Properties] = None) : Connection

    /**
     * Returns a set of common properties
     *
     * @param context
     * @return
     */
    override protected def instanceProperties(context: Context, properties:Option[Connection.Properties]): Connection.Properties = {
        require(context != null)
        val name = context.evaluate(this.name)
        val props = Connection.Properties(
            context,
            metadata.map(_.instantiate(context, name, Category.CONNECTION, kind)).getOrElse(Metadata(context, name, Category.CONNECTION, kind))
        )
        properties.map(p => props.merge(p)).getOrElse(props)
    }
}



class ConnectionSpecAnnotationHandler extends ClassAnnotationHandler {
    override def annotation: Class[_] = classOf[ConnectionType]

    override def register(clazz: Class[_]): Unit =
        ConnectionSpec.register(clazz.getAnnotation(classOf[ConnectionType]).kind(), clazz.asInstanceOf[Class[_ <: ConnectionSpec]])
}
