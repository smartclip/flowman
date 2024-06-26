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

package com.dimajix.flowman.server.model


final case class Resource(
    category:String,
    name:String,
    partition:Map[String,String]
)

final case class Node(
    id:Int,
    category:String,
    kind:String,
    name:String,
    provides:Seq[Resource],
    requires:Seq[Resource]
)

final case class Edge(
    input:Int,
    output:Int,
    action:String,
    labels:Map[String,Seq[String]]
)

final case class Graph(
    nodes:Seq[Node],
    edges:Seq[Edge]
)
