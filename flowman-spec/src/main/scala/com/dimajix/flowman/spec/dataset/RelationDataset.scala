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

package com.dimajix.flowman.spec.dataset

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.spark.sql.DataFrame

import com.dimajix.common.Trilean
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.execution.OutputMode
import com.dimajix.flowman.model.AbstractInstance
import com.dimajix.flowman.model.Dataset
import com.dimajix.flowman.model.RelationIdentifier
import com.dimajix.flowman.model.ResourceIdentifier
import com.dimajix.flowman.types.SingleValue
import com.dimajix.flowman.types.StructType


object RelationDataset {
    def apply(context: Context, relation:RelationIdentifier, partition:Map[String,SingleValue]) : RelationDataset = {
        new RelationDataset(
            Dataset.Properties(context),
            relation,
            partition
        )
    }
}
case class RelationDataset(
    instanceProperties: Dataset.Properties,
    relation: RelationIdentifier,
    partition:Map[String,SingleValue]
) extends AbstractInstance with Dataset {
    /**
      * Returns a list of physical resources produced by writing to this dataset
      * @return
      */
    override def provides : Set[ResourceIdentifier] = {
        val instance = context.getRelation(relation)
        instance.provides ++ instance.resources(partition)
    }

    /**
     * Returns a list of physical resources required for reading from this dataset
     * @return
     */
    override def requires : Set[ResourceIdentifier] = {
        val instance = context.getRelation(relation)
        instance.provides ++ instance.requires ++ instance.resources(partition)
    }

    /**
      * Returns true if the data represented by this Dataset actually exists
      *
      * @param execution
      * @return
      */
    override def exists(execution: Execution): Trilean = {
        val instance = context.getRelation(relation)
        instance.loaded(execution, partition)
    }

    /**
      * Removes the data represented by this dataset, but leaves the underlying relation present
      *
      * @param execution
      */
    override def clean(execution: Execution): Unit = {
        val instance = context.getRelation(relation)
        instance.truncate(execution, partition)
    }

    /**
      * Reads data from the relation, possibly from specific partitions
      *
      * @param execution
      * @param schema - the schema to read. If none is specified, all available columns will be read
      * @return
      */
    override def read(execution: Execution, schema: Option[org.apache.spark.sql.types.StructType]): DataFrame = {
        val instance = context.getRelation(relation)
        instance.read(execution, schema, partition)
    }

    /**
      * Writes data into the relation, possibly into a specific partition
      *
      * @param execution
      * @param df - dataframe to write
      */
    override def write(execution: Execution, df: DataFrame, mode: OutputMode): Unit = {
        val instance = context.getRelation(relation)
        // Remove partition columns
        val outDf = partition.keys.foldLeft(df)((df,col) => df.drop(col))
        instance.write(execution, outDf, partition, mode)
    }

    /**
      * Returns the schema as produced by this dataset. The schema will not include any partition columns
      *
      * @return
      */
    override def describe(execution:Execution) : Option[StructType] = {
        val instance = context.getRelation(relation)
        Some(instance.describe(execution))
    }
}


class RelationDatasetSpec extends DatasetSpec {
    @JsonProperty(value="relation", required = true) private var relation: String = _
    @JsonProperty(value="partition", required = false) private var partition: Map[String, String] = Map()

    override def instantiate(context: Context): RelationDataset = {
        val id = RelationIdentifier(context.evaluate(relation))
        RelationDataset(
            instanceProperties(context, id.toString),
            id,
            partition.map { case(n,p) => (n,SingleValue(context.evaluate(p))) }
        )
    }
}
