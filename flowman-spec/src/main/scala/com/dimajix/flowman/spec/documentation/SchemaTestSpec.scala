/*
 * Copyright 2022 Kaya Kupferschmidt
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

package com.dimajix.flowman.spec.documentation

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo

import com.dimajix.common.TypeRegistry
import com.dimajix.flowman.documentation.SchemaReference
import com.dimajix.flowman.documentation.SchemaTest
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.spec.annotation.SchemaTestType
import com.dimajix.flowman.spi.ClassAnnotationHandler


object SchemaTestSpec extends TypeRegistry[SchemaTestSpec] {
}


@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "kind")
@JsonSubTypes(value = Array(
    new JsonSubTypes.Type(name = "file", value = classOf[FileGeneratorSpec])
))
abstract class SchemaTestSpec {
    def instantiate(context: Context, parent:SchemaReference): SchemaTest
}



class SchemaTestSpecAnnotationHandler extends ClassAnnotationHandler {
    override def annotation: Class[_] = classOf[SchemaTestType]

    override def register(clazz: Class[_]): Unit =
        SchemaTestSpec.register(clazz.getAnnotation(classOf[SchemaTestType]).kind(), clazz.asInstanceOf[Class[_ <: SchemaTestSpec]])
}