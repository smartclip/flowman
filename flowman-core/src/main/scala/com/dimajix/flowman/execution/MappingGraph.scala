/*
 * Copyright 2019 Kaya Kupferschmidt
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

import com.dimajix.flowman.model.{MappingOutputIdentifier, Target}

import scala.collection.mutable

case class Node( mappingOutputIdentifier: MappingOutputIdentifier, parents: mutable.Set[Node], children: mutable.Set[Node] ) {}

class MappingGraph {
    val nodeMapping: mutable.Map[MappingOutputIdentifier, Node] = mutable.Map()

    def getJobsDependencies( phase: Phase, target: Seq[Target] ) = {
        target
            .map( target => {
                val dependencyNode = getTargetsDependencyGraph( target, phase )

                ( target, dependencyNode )
            })
    }

    def getTargetsDependencyGraph( target: Target, phase: Phase ): Seq[Node] = {
        target
            .mappings( phase )
            .toSeq
            .map( mapping => {
                getNode( mapping, target.context )
            })
    }

    def getNode ( mappingOutputIdentifier: MappingOutputIdentifier, context: Context ): Node = {
        createDependencyGraphDepthFirst( mappingOutputIdentifier, context, Set.empty )
    }

    private def createDependencyGraphDepthFirst (
        mappingOutputIdentifier: MappingOutputIdentifier,
        context: Context,
        visitedMappings: Set[MappingOutputIdentifier]
    ): Node = {
        // check if node has been seen before while traversing current branch
        // "visitedMappings" are only dedicated to single branches (not global)
        // therefore first reencounter of a mappingOutputIdentifier indicates a loop
        if ( visitedMappings.contains( mappingOutputIdentifier ) ) {
            throw new IllegalArgumentException
        }

        nodeMapping.get( mappingOutputIdentifier ) match {
            case Some( node ) => { node }
            case None => {
                val mapping = context.getMapping( mappingOutputIdentifier.mapping )
                val children = mapping
                    .inputs
                    .map( mappingOutputIdentifier => {
                        createDependencyGraphDepthFirst( mappingOutputIdentifier, context, visitedMappings + mapping.output )
                    })

                val node = Node( mappingOutputIdentifier, mutable.Set[Node]() , mutable.Set( children:_* ) )
                children.foreach( childNode => childNode.parents += node )
                nodeMapping.update( mappingOutputIdentifier, node )

                node
            }
        }
    }
}
