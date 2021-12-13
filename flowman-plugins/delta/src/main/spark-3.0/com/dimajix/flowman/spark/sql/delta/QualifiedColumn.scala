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

package com.dimajix.flowman.spark.sql.delta

import org.apache.spark.sql.catalyst.plans.logical.QualifiedColType
import org.apache.spark.sql.types.DataType


object QualifiedColumn {
    def apply(name:String, catalogType:DataType, nullable:Boolean, description:Option[String]) : QualifiedColType =
        QualifiedColType(Seq(name), catalogType, nullable, description, None)
}
