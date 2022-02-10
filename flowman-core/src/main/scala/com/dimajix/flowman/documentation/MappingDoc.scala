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

package com.dimajix.flowman.documentation

import com.dimajix.flowman.model.MappingIdentifier
import com.dimajix.flowman.model.MappingOutputIdentifier


final case class MappingOutputReference(
    override val parent:Option[Reference],
    name:String
) extends Reference {
    override def toString: String = {
        parent match {
            case Some(ref) => ref.toString + "/output=" + name
            case None => name
        }
    }
}


final case class MappingOutputDoc(
    parent:Some[Reference],
    identifier: MappingOutputIdentifier,
    description: Option[String],
    schema:Option[SchemaDoc]
) extends Fragment {
    override def reference: Reference = MappingOutputReference(parent, identifier.output)
    override def fragments: Seq[Fragment] = schema.toSeq
    override def reparent(parent: Reference): MappingOutputDoc = {
        val ref = MappingOutputReference(Some(parent), identifier.output)
        copy(
            parent=Some(parent),
            schema=schema.map(_.reparent(ref))
        )
    }

    def merge(other:MappingOutputDoc) : MappingOutputDoc = {
        val id = if (identifier.mapping.isEmpty) other.identifier else identifier
        val desc = other.description.orElse(this.description)
        val schm = schema.map(_.merge(other.schema)).orElse(other.schema)
        val result = copy(identifier=id, description=desc, schema=schm)
        parent.orElse(other.parent)
            .map(result.reparent)
            .getOrElse(result)
    }
}


final case class MappingReference(
    override val parent:Option[Reference],
    name:String
) extends Reference {
    override def toString: String = {
        parent match {
            case Some(ref) => ref.toString + "/mapping=" + name
            case None => name
        }
    }
}


final case class MappingDoc(
    parent:Option[Reference],
    identifier:MappingIdentifier,
    description:Option[String],
    inputs:Seq[Reference],
    outputs:Seq[MappingOutputDoc]
) extends EntityDoc {
    override def reference: MappingReference = MappingReference(parent, identifier.name)
    override def fragments: Seq[Fragment] = outputs
    override def reparent(parent: Reference): MappingDoc = {
        val ref = MappingOutputReference(Some(parent), identifier.name)
        copy(
            parent=Some(parent),
            outputs=outputs.map(_.reparent(ref))
        )
    }

    def merge(other:Option[MappingDoc]) : MappingDoc = other.map(merge).getOrElse(this)
    def merge(other:MappingDoc) : MappingDoc = {
        val id = if (identifier.isEmpty) other.identifier else identifier
        val desc = other.description.orElse(this.description)
        val in = inputs.toSet ++ other.inputs.toSet
        val out = outputs.map { out =>
                other.outputs.find(_.identifier.output == out.identifier.output).map(out.merge).getOrElse(out)
            } ++
            other.outputs.filter(out => !outputs.exists(_.identifier.output == out.identifier.output))
        val result = copy(identifier=id, description=desc, inputs=in.toSeq, outputs=out)
        parent.orElse(other.parent)
            .map(result.reparent)
            .getOrElse(result)
    }
}