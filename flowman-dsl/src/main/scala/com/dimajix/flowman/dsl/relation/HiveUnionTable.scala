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

package com.dimajix.flowman.dsl.relation

import org.apache.hadoop.fs.Path

import com.dimajix.flowman.catalog.TableIdentifier
import com.dimajix.flowman.dsl.RelationGen
import com.dimajix.flowman.model.PartitionField
import com.dimajix.flowman.model.Relation
import com.dimajix.flowman.model.Schema
import com.dimajix.flowman.model.Prototype
import com.dimajix.flowman.spec.relation.HiveUnionTableRelation


case class HiveUnionTable(
    schema:Option[Prototype[Schema]] = None,
    partitions: Seq[PartitionField] = Seq(),
    tableDatabase: Option[String] = None,
    tablePrefix: String,
    locationPrefix: Option[Path] = None,
    viewDatabase: Option[String] = None,
    view: String,
    external: Boolean = false,
    format: Option[String] = None,
    options: Map[String,String] = Map(),
    rowFormat: Option[String] = None,
    inputFormat: Option[String] = None,
    outputFormat: Option[String] = None,
    properties: Map[String, String] = Map(),
    serdeProperties: Map[String, String] = Map()
) extends RelationGen {
    override def apply(props:Relation.Properties) : HiveUnionTableRelation = {
        val context = props.context
        HiveUnionTableRelation(
            props,
            schema.map(_.instantiate(context)),
            partitions,
            TableIdentifier(tablePrefix, tableDatabase),
            locationPrefix,
            TableIdentifier(view, viewDatabase),
            external,
            format,
            options,
            rowFormat,
            inputFormat,
            outputFormat,
            properties,
            serdeProperties
        )
    }
}
