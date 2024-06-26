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

import com.dimajix.flowman.graph.Graph
import com.dimajix.flowman.model.Mapping
import com.dimajix.flowman.model.MappingIdentifier
import com.dimajix.flowman.model.Relation
import com.dimajix.flowman.model.RelationIdentifier


class ReferenceResolver(graph:Graph) {
    /**
     * Resolve a mapping via its documentation reference in the graph
     * @param graph
     * @param ref
     * @return
     */
    def resolve(ref:MappingReference) : Option[Mapping] = {
        ref.parent match {
            case None =>
                graph.mappings.find(m => m.name == ref.name).map(_.mapping)
            case Some(ProjectReference(project)) =>
                val id = MappingIdentifier(ref.name, project)
                graph.mappings.find(m => m.identifier == id).map(_.mapping)
            case _ => None
        }
    }

    /**
     * Resolve a relation via its documentation reference in the graph
     * @param graph
     * @param ref
     * @return
     */
    def resolve(ref:RelationReference) : Option[Relation] = {
        // TODO: Also resolve nested relations!
        ref.parent match {
            case None =>
                graph.relations.find(m => m.name == ref.name).map(_.relation)
            case Some(ProjectReference(project)) =>
                val id = RelationIdentifier(ref.name, project)
                graph.relations.find(m => m.identifier == id).map(_.relation)
            case _ => None
        }
    }
}
