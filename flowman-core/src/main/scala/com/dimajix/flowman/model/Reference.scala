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

import com.dimajix.flowman.execution.Context


abstract class Reference[T] {
    val value:T
    def name:String
    def identifier:Identifier[T]
}

final case class ValueRelationReference(context:Context, template:Prototype[Relation]) extends Reference[Relation] {
    override lazy val value : Relation = template.instantiate(context)
    override def name: String = value.name
    override def identifier: RelationIdentifier = value.identifier
}
final case class IdentifierRelationReference(context: Context, relation:RelationIdentifier) extends Reference[Relation] {
    override lazy val value: Relation = context.getRelation(relation)
    override def name: String = relation.name
    override def identifier: RelationIdentifier = relation
}