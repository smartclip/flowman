/*
 * Copyright (C) 2021 The Flowman Authors
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

package com.dimajix.flowman.spec.target

import java.time.Duration

import com.fasterxml.jackson.annotation.JsonProperty
import io.delta.tables.DeltaTable
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkShim
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.lit
import org.slf4j.LoggerFactory

import com.dimajix.common.No
import com.dimajix.common.Trilean
import com.dimajix.common.Unknown
import com.dimajix.flowman.common.ThreadUtils
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.execution.Operation
import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.SparkBackend
import com.dimajix.flowman.graph.Linker
import com.dimajix.flowman.model.BaseTarget
import com.dimajix.flowman.model.Reference
import com.dimajix.flowman.model.Relation
import com.dimajix.flowman.model.ResourceIdentifier
import com.dimajix.flowman.model.Target
import com.dimajix.flowman.spec.annotation.TargetType
import com.dimajix.flowman.spec.relation.DeltaFileRelation
import com.dimajix.flowman.spec.relation.DeltaTableRelation
import com.dimajix.flowman.spec.relation.RelationReferenceSpec
import com.dimajix.flowman.types.SingleValue


case class DeltaVacuumTarget(
    override val instanceProperties:Target.Properties,
    relation:Reference[Relation],
    retentionTime:Option[Duration] = None,
    compaction:Boolean = false,
    minFiles:Int = 32,
    maxFiles:Int = 64
) extends BaseTarget {
    /**
     * Returns all phases which are implemented by this target in the execute method
     *
     * @return
     */
    override def phases: Set[Phase] = Set(Phase.BUILD)

    /**
     * Returns a list of physical resources required by this target
     *
     * @return
     */
    override def requires(phase: Phase): Set[ResourceIdentifier] = {
        phase match {
            case Phase.BUILD =>
                val rel = relation.value
                rel.provides(Operation.CREATE) ++ rel.provides(Operation.WRITE)
            case _ => Set.empty
        }
    }

    /**
     * Returns the state of the target, specifically of any artifacts produces. If this method return [[Yes]],
     * then an [[execute]] should update the output, such that the target is not 'dirty' any more.
     *
     * @param execution
     * @param phase
     * @return
     */
    override def dirty(execution: Execution, phase: Phase): Trilean = {
        phase match {
            case Phase.BUILD => Unknown
            case _ => No
        }
    }

    /**
     * Abstract method which will perform the output operation. This method may throw an exception, which will be
     * caught an wrapped in the final [[TargetResult]].
     *
     * @param execution
     */
    private def isConnect(execution: Execution) : Boolean = {
        SparkBackend.of(execution.spark).isConnect
    }

    private def sqlIdentifier : String = {
        relation.value match {
            case table:DeltaTableRelation => table.table.quotedString
            case files:DeltaFileRelation => s"delta.`${files.location.toString}`"
            case rel:Relation => throw new IllegalArgumentException(s"DeltaVacuumTarget only supports relations of type deltaTable and deltaFiles, but it was given relation '${rel.identifier}' of kind '${rel.kind}'")
        }
    }

    private def sourceDf(execution: Execution) : DataFrame = {
        relation.value match {
            case table:DeltaTableRelation => execution.spark.table(table.table.quotedString)
            case files:DeltaFileRelation => execution.spark.read.format("delta").load(files.location.toString)
            case rel:Relation => throw new IllegalArgumentException(s"DeltaVacuumTarget only supports relations of type deltaTable and deltaFiles, but it was given relation '${rel.identifier}' of kind '${rel.kind}'")
        }
    }

    override protected def build(execution: Execution): Unit = {
        val connect = isConnect(execution)
        val deltaTable = if (connect)
            None
        else {
            Some(relation.value match {
                case table:DeltaTableRelation => DeltaTable.forName(execution.spark, table.table.toString())
                case files:DeltaFileRelation => DeltaTable.forPath(execution.spark, files.location.toString)
                case rel:Relation => throw new IllegalArgumentException(s"DeltaVacuumTarget only supports relations of type deltaTable and deltaFiles, but it was given relation '${rel.identifier}' of kind '${rel.kind}'")
            })
        }

        if (compaction) {
            deltaTable match {
                case Some(dt) => compact(dt)
                case None => compactConnect(execution)
            }
        }

        logger.info(s"Vacuum Delta relation '${relation.identifier}'")
        retentionTime match {
            case Some(duration) =>
                deltaTable match {
                    case Some(dt) => dt.vacuum(duration.toHours)
                    case None => execution.spark.sql(s"VACUUM $sqlIdentifier RETAIN ${duration.toHours} HOURS").collect()
                }
            case None =>
                deltaTable match {
                    case Some(dt) => dt.vacuum()
                    case None => execution.spark.sql(s"VACUUM $sqlIdentifier").collect()
                }
        }
    }

    /**
     * Creates all known links for building a descriptive graph of the whole data flow
     * Params: linker - The linker object to use for creating new edges
     */
    override def link(linker: Linker, phase:Phase): Unit = {
        if (phase == Phase.BUILD) {
            linker.write(relation, Map.empty[String,SingleValue])
        }
    }

    private def compact(deltaTable:DeltaTable) : Unit = {
        val spark = deltaTable.toDF.sparkSession
        val deltaLog = relation.value match {
            case table:DeltaTableRelation => DeltaLog.forTable(spark, table.table.toSpark)
            case files:DeltaFileRelation => DeltaLog.forTable(spark, files.location.toString)
            case rel:Relation => throw new IllegalArgumentException(s"DeltaVacuumTarget only supports relations of type deltaTable and deltaFiles, but it was given relation '${rel.identifier}' of kind '${rel.kind}'")
        }

        val partitionColumns = deltaLog.snapshot.metadata.partitionColumns
        if (partitionColumns.nonEmpty) {
            compactPartitioned(deltaTable, deltaLog)
        }
        else {
            compactUnpartitioned(deltaTable, deltaLog)
        }
    }

    /**
      * Compacts a Delta table in Connect mode. DeltaLog (which is used in classic mode to count files and to group
      * them by partition) is not reachable from a Connect client, so the number of files is taken from the
      * `DESCRIBE DETAIL` SQL command that runs on the server. Per-partition file counts are not exposed by
      * `DESCRIBE DETAIL`; when the table contains too many files the whole table is rewritten (a partitioned
      * overwrite only touches the partitions present in the DataFrame).
      */
    private def compactConnect(execution: Execution) : Unit = {
        val row = execution.spark.sql(s"DESCRIBE DETAIL $sqlIdentifier").head()
        val fileCount = row.getAs[Long]("numFiles")
        if (fileCount > maxFiles) {
            logger.info(s"Compacting Delta relation '${relation.identifier}'")
            val df = sourceDf(execution).repartition(minFiles)
            writeTable(df, None)
        }
    }

    private def writeTable(df:DataFrame, filter:Option[String]) : Unit = {
        val writer = df.write
            .format("delta")
            .mode("overwrite")
            .option("dataChange", "false")
        filter.map(writer.option("replaceWhere", _))

        relation.value match {
            case table:DeltaTableRelation => writer.insertInto(table.table.toString())
            case files:DeltaFileRelation => writer.save(files.location.toString)
            case rel:Relation => throw new IllegalArgumentException(s"DeltaVacuumTarget only supports relations of type deltaTable and deltaFiles, but it was given relation '${rel.identifier}' of kind '${rel.kind}'")
        }
    }

    private def compactUnpartitioned(deltaTable:DeltaTable, deltaLog:DeltaLog) : Unit = {
        val currentFileCount = deltaLog
            .snapshot
            .allFiles
            .toDF
            .count()
        if (currentFileCount > maxFiles) {
            logger.info(s"Compacting Delta relation '${relation.identifier}'")
            val df = deltaTable.toDF
                .repartition(minFiles)
            writeTable(df, None)
        }
    }

    private def compactPartitioned(deltaTable:DeltaTable, deltaLog:DeltaLog) : Unit = {
        val partitionColumns = deltaLog.snapshot.metadata.partitionColumns
        val compactionPartitionList = deltaLog
            .snapshot
            .allFiles
            .toDF
            .groupBy(partitionColumns.map(p => col("partitionValues." + p)): _*)
            .agg(
                functions.count("*").as("count"),
                functions.max("modificationTime").as("modificationTime")
            )
            .where(col("count") > maxFiles)
            //.where(s"`max(modificationTime)` <= ${compactionThresholdEpochDay}")
            .select(partitionColumns.map(col): _*)
            .sort(partitionColumns.map(col): _*)
            .collect

        ThreadUtils.parmap(compactionPartitionList, "delta-vacuum-", 8) { partition =>
            val partitionFilter = partition.toSeq.zip(partitionColumns)
                    .map { case (v, k) => col(k) === lit(v) }
                    .reduce(_ && _)

            val partitionString = partition.toSeq.zip(partitionColumns).map(kv => kv._2 + "=" + kv._1.toString).mkString(",")
            val partitionFilterSql = SparkShim.expr(partitionFilter).sql
            logger.info(s"Compacting Delta relation '${relation.identifier}' for partition ${partitionString}")
            val df = deltaTable.toDF
                .where(partitionFilterSql)
                .repartition(minFiles)
            writeTable(df, Some(partitionFilterSql))
        }
    }
}


@TargetType(kind="deltaVacuum")
class DeltaVacuumTargetSpec extends TargetSpec {
    @JsonProperty(value = "relation", required = true) private var relation: RelationReferenceSpec = _
    @JsonProperty(value = "retentionTime", required = true) private var retentionTime: Option[String] = None
    @JsonProperty(value = "compaction", required = false) private var compaction: String = "false"
    @JsonProperty(value = "minFiles", required = false) private var minFiles: String = "16"
    @JsonProperty(value = "maxFiles", required = false) private var maxFiles: String = "64"

    override def instantiate(context: Context, properties:Option[Target.Properties]): DeltaVacuumTarget = {
        DeltaVacuumTarget(
            instanceProperties(context, properties),
            relation.instantiate(context),
            context.evaluate(retentionTime).map(Duration.parse),
            context.evaluate(compaction).toBoolean,
            context.evaluate(minFiles).toInt,
            context.evaluate(maxFiles).toInt
        )
    }
}
