/*
 * Copyright 2018-2019 Kaya Kupferschmidt
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

package com.dimajix.flowman.history

import java.sql.Timestamp
import java.time.ZoneId
import java.time.ZonedDateTime
import java.util.Locale
import java.util.Properties

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.language.higherKinds

import org.slf4j.LoggerFactory
import slick.jdbc.JdbcProfile

import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.Status


private[history] object JdbcStateRepository {
    private val logger = LoggerFactory.getLogger(classOf[JdbcStateRepository])

    case class JobRun(
        id:Long,
        namespace: String,
        project:String,
        version:String,
        job:String,
        phase:String,
        args_hash:String,
        start_ts:Option[Timestamp],
        end_ts:Option[Timestamp],
        status:String,
        error:Option[String]
    ) extends JobToken

    case class JobArgument(
        job_id:Long,
        name:String,
        value:String
    )

    case class JobMetricLabel(
        metric_id:Long,
        name:String,
        value:String
    )
    case class JobMetric(
        id:Long,
        job_id:Long,
        name:String,
        ts:Timestamp,
        value:Double
    )

    case class TargetRun(
        id:Long,
        job_id:Option[Long],
        namespace: String,
        project:String,
        version:String,
        target:String,
        phase:String,
        partitions_hash:String,
        start_ts:Option[Timestamp],
        end_ts:Option[Timestamp],
        status:String,
        error:Option[String]
    ) extends TargetToken

    case class TargetPartition(
        target_id:Long,
        name:String,
        value:String
    )
}


private[history] class JdbcStateRepository(connection: JdbcStateStore.Connection, val profile:JdbcProfile) {
    import profile.api._

    import JdbcStateRepository._

    private lazy val db = {
        val url = connection.url
        val driver = connection.driver
        val user = connection.user
        val password = connection.password
        val props = new Properties()
        connection.properties.foreach(kv => props.setProperty(kv._1, kv._2))
        logger.debug(s"Connecting via JDBC to $url with driver $driver")
        val executor = slick.util.AsyncExecutor(
            name="Flowman.default",
            minThreads = 20,
            maxThreads = 20,
            queueSize = 1000,
            maxConnections = 20)
        Database.forURL(url, driver=driver, user=user.orNull, password=password.orNull, prop=props, executor=executor)
    }

    val jobRuns = TableQuery[JobRuns]
    val jobArgs = TableQuery[JobArguments]
    val jobMetrics = TableQuery[JobMetrics]
    val jobMetricLabels = TableQuery[JobMetricLabels]
    val targetRuns = TableQuery[TargetRuns]
    val targetPartitions = TableQuery[TargetPartitions]

    class JobRuns(tag:Tag) extends Table[JobRun](tag, "JOB_RUN") {
        def id = column[Long]("id", O.PrimaryKey, O.AutoInc)
        def namespace = column[String]("namespace")
        def project = column[String]("project")
        def version = column[String]("version")
        def job = column[String]("job")
        def phase = column[String]("phase")
        def args_hash = column[String]("args_hash")
        def start_ts = column[Option[Timestamp]]("start_ts")
        def end_ts = column[Option[Timestamp]]("end_ts")
        def status = column[String]("status")
        def error = column[Option[String]]("error")

        def idx = index("JOB_RUN_IDX", (namespace, project, job, phase, args_hash, status), unique = false)

        def * = (id, namespace, project, version, job, phase, args_hash, start_ts, end_ts, status, error) <> (JobRun.tupled, JobRun.unapply)
    }

    class JobArguments(tag: Tag) extends Table[JobArgument](tag, "JOB_ARGUMENT") {
        def job_id = column[Long]("job_id")
        def name = column[String]("name")
        def value = column[String]("value")

        def pk = primaryKey("JOB_ARGUMENT_PK", (job_id, name))
        def job = foreignKey("JOB_ARGUMENT_JOB_FK", job_id, jobRuns)(_.id, onUpdate=ForeignKeyAction.Restrict, onDelete=ForeignKeyAction.Cascade)

        def * = (job_id, name, value) <> (JobArgument.tupled, JobArgument.unapply)
    }

    class JobMetrics(tag: Tag) extends Table[JobMetric](tag, "JOB_METRIC") {
        def id = column[Long]("metric_id", O.PrimaryKey, O.AutoInc)
        def job_id = column[Long]("job_id")
        def name = column[String]("name")
        def ts = column[Timestamp]("ts")
        def value = column[Double]("value")

        def job = foreignKey("JOB_METRIC_JOB_FK", job_id, jobRuns)(_.id, onUpdate=ForeignKeyAction.Restrict, onDelete=ForeignKeyAction.Cascade)

        def * = (id, job_id, name, ts, value) <> (JobMetric.tupled, JobMetric.unapply)
    }

    class JobMetricLabels(tag: Tag) extends Table[JobMetricLabel](tag, "JOB_METRIC_LABEL") {
        def metric_id = column[Long]("metric_id")
        def name = column[String]("name")
        def value = column[String]("value")

        def pk = primaryKey("JOB_METRIC_LABEL_PK", (metric_id, name))
        def metric = foreignKey("JOB_METRIC_LABEL_JOB_METRIC_FK", metric_id, jobMetrics)(_.id, onUpdate=ForeignKeyAction.Restrict, onDelete=ForeignKeyAction.Cascade)
        def idx = index("JOB_METRIC_LABEL_IDX", (name, value), unique = false)

        def * = (metric_id, name, value) <> (JobMetricLabel.tupled, JobMetricLabel.unapply)
    }

    class TargetRuns(tag: Tag) extends Table[TargetRun](tag, "TARGET_RUN") {
        def id = column[Long]("id", O.PrimaryKey, O.AutoInc)
        def job_id = column[Option[Long]]("job_id")
        def namespace = column[String]("namespace")
        def project = column[String]("project")
        def version = column[String]("version")
        def target = column[String]("target")
        def phase = column[String]("phase")
        def partitions_hash = column[String]("partitions_hash")
        def start_ts = column[Option[Timestamp]]("start_ts")
        def end_ts = column[Option[Timestamp]]("end_ts")
        def status = column[String]("status")
        def error = column[Option[String]]("error")

        def idx = index("TARGET_RUN_IDX", (namespace, project, target, phase, partitions_hash, status), unique = false)
        def job = foreignKey("TARGET_RUN_JOB_RUN_FK", job_id, jobRuns)(_.id.?, onUpdate=ForeignKeyAction.Restrict, onDelete=ForeignKeyAction.Cascade)

        def * = (id, job_id, namespace, project, version, target, phase, partitions_hash, start_ts, end_ts, status, error) <> (TargetRun.tupled, TargetRun.unapply)
    }

    class TargetPartitions(tag: Tag) extends Table[TargetPartition](tag, "TARGET_PARTITION") {
        def target_id = column[Long]("target_id")
        def name = column[String]("name")
        def value = column[String]("value")

        def pk = primaryKey("TARGET_PARTITION_PK", (target_id, name))
        def target = foreignKey("TARGET_PARTITION_TARGET_RUN_FK", target_id, targetRuns)(_.id, onUpdate=ForeignKeyAction.Restrict, onDelete=ForeignKeyAction.Cascade)

        def * = (target_id, name, value) <> (TargetPartition.tupled, TargetPartition.unapply)
    }

    implicit class QueryWrapper[E,U,C[_]] (query:Query[E, U, C]) {
        def optionalFilter[T](value:Option[T])(f:(E,T) => Rep[Boolean]) : Query[E,U,C] = {
            if (value.nonEmpty)
                query.filter(v => f(v,value.get))
            else
                query
        }
        def optionalFilter2[T](value:Option[T])(f:(E,T) => Rep[Option[Boolean]]) : Query[E,U,C] = {
            if (value.nonEmpty)
                query.filter(v => f(v,value.get))
            else
                query
        }
    }

    def create() : Unit = {
        import scala.concurrent.ExecutionContext.Implicits.global
        val tables = Seq(jobRuns, jobArgs, jobMetrics, jobMetricLabels, targetRuns, targetPartitions)

        val existing = db.run(profile.defaultTables)
        val query = existing.flatMap( v => {
            val names = v.map(mt => mt.name.name.toLowerCase(Locale.ROOT))
            val createIfNotExist = tables
                .filter(table => !names.contains(table.baseTableRow.tableName.toLowerCase(Locale.ROOT)))
                .map(_.schema.create)
            db.run(DBIO.sequence(createIfNotExist))
        })
        Await.result(query, Duration.Inf)
    }

    def getJobState(run:JobRun) : Option[JobState] = {
        val latestId = jobRuns
            .filter(r => r.namespace === run.namespace
                && r.project === run.project
                && r.job === run.job
                && r.args_hash === run.args_hash
                && r.status =!= Status.SKIPPED.toString
            ).map(_.id)
            .max

        val qj = jobRuns.filter(_.id === latestId)
        val job = Await.result(db.run(qj.result), Duration.Inf)
            .headOption

        // Retrieve job arguments
        val qa = job.map(j => jobArgs.filter(_.job_id === j.id))
        val args = qa.toSeq.flatMap(q =>
            Await.result(db.run(q.result), Duration.Inf)
                .map(a => (a.name, a.value))
        ).toMap

        job.map(state => JobState(
            state.id.toString,
            state.namespace,
            state.project,
            state.version,
            state.job,
            Phase.ofString(state.phase),
            args,
            Status.ofString(state.status),
            state.start_ts.map(_.toInstant.atZone(ZoneId.of("UTC"))),
            state.end_ts.map(_.toInstant.atZone(ZoneId.of("UTC"))),
            state.error
        ))
    }

    def setJobStatus(run:JobRun) : Unit = {
        val q = jobRuns.filter(_.id === run.id).map(r => (r.end_ts, r.status, r.error)).update((run.end_ts, run.status, run.error))
        Await.result(db.run(q), Duration.Inf)
    }

    def insertJobRun(run:JobRun, args:Map[String,String]) : JobRun = {
        val runQuery = (jobRuns returning jobRuns.map(_.id) into((run, id) => run.copy(id=id))) += run
        val runResult = Await.result(db.run(runQuery), Duration.Inf)

        val runArgs = args.map(kv => JobArgument(runResult.id, kv._1, kv._2))
        val argsQuery = jobArgs ++= runArgs
        Await.result(db.run(argsQuery), Duration.Inf)

        runResult
    }

    def insertJobMetrics(run:JobRun, metrics:Seq[Measurement]) : Unit = {
        metrics.foreach { m =>
            val jobMetric = JobMetric(0, run.id, m.name, new Timestamp(m.ts.toInstant.toEpochMilli), m.value)
            val jmQuery = (jobMetrics returning jobMetrics.map(_.id) into((jm,id) => jm.copy(id=id))) += jobMetric
            val jmResult = Await.result(db.run(jmQuery), Duration.Inf)

            val labels = m.labels.map(l => JobMetricLabel(jmResult.id, l._1, l._2))
            val mlQuery = jobMetricLabels ++= labels
            Await.result(db.run(mlQuery), Duration.Inf)
        }
    }

    def getJobMetrics(jobId:Long) : Seq[Measurement] = {
        // Ensure that job actually exists
        val jq = jobRuns.filter(_.id === jobId)
        Await.result(db.run(jq.result), Duration.Inf).head

        // Now query metrics
        val q = jobMetrics.filter(_.job_id === jobId)
            .joinLeft(jobMetricLabels).on(_.id === _.metric_id)
        Await.result(db.run(q.result), Duration.Inf)
            .groupBy(_._1.id)
            .map { g =>
                val metric = g._2.head._1
                val labels = g._2.flatMap(_._2).map(kv => kv.name -> kv.value).toMap
                Measurement(metric.name, metric.ts.toInstant.atZone(ZoneId.of("UTC")), labels, metric.value)
            }
            .toSeq
    }

    private def queryJobs(query:JobQuery) : Query[JobRuns,JobRun,Seq]  = {
        query.args.foldLeft(jobRuns.map(identity))((q, kv) => q
                .join(jobArgs).on(_.id === _.job_id)
                .filter(a => a._2.name === kv._1 && a._2.value === kv._2)
                .map(xy => xy._1)
            )
            .optionalFilter(query.id)(_.id === _.toLong)
            .optionalFilter(query.namespace)(_.namespace === _)
            .optionalFilter(query.project)(_.project === _)
            .optionalFilter(query.job)(_.job === _)
            .optionalFilter(query.status)(_.status === _.toString)
            .optionalFilter(query.phase)(_.phase === _.toString)
            .optionalFilter(query.from)((e,v) => e.start_ts.getOrElse(new Timestamp(0)) >= Timestamp.from(v.toInstant))
            .optionalFilter(query.to)((e,v) => e.start_ts.getOrElse(new Timestamp(0)) <= Timestamp.from(v.toInstant))
    }

    def findJobs(query:JobQuery, order:Seq[JobOrder], limit:Int, offset:Int) : Seq[JobState] = {
        def mapOrderColumn(order:JobOrder) : JobRuns => Rep[_] = {
            order.column match {
                case JobColumn.DATETIME => t => t.start_ts
                case JobColumn.ID => t => t.id
                case JobColumn.PROJECT => t => t.project
                case JobColumn.NAME => t => t.job
                case JobColumn.PHASE => t => t.phase
                case JobColumn.STATUS => t => t.status
            }
        }
        def mapOrderDirection(order:JobOrder) : slick.ast.Ordering = {
            if (order.isAscending)
                slick.ast.Ordering(slick.ast.Ordering.Asc)
            else
                slick.ast.Ordering(slick.ast.Ordering.Desc)
        }
        val ordering =
            (l:JobState, r:JobState) => {
                order.map { o =>
                    val result = o.column match {
                        case JobColumn.DATETIME => cmpOpt(l.startDateTime, r.startDateTime)(cmpDt)
                        case JobColumn.ID => cmpStr(l.id, r.id)
                        case JobColumn.NAME => cmpStr(l.job, r.job)
                        case JobColumn.PHASE => cmpStr(l.phase.toString, r.phase.toString)
                        case JobColumn.STATUS => cmpStr(l.status.toString, r.status.toString)
                    }
                    if (o.isAscending)
                        result
                    else
                        -result
                }
                .find(_ != 0)
                .exists(_ < 0)
            }

        val q = queryJobs(query)
            .sorted(job => new slick.lifted.Ordered(order.map(o => (mapOrderColumn(o)(job).toNode, mapOrderDirection(o))).toVector))
            .drop(offset)
            .take(limit)
            .joinLeft(jobArgs).on(_.id === _.job_id)

        Await.result(db.run(q.result), Duration.Inf)
            .groupBy(_._1.id)
            .values
            .map { states =>
                val state = states.head._1
                val args = states.flatMap(_._2)
                JobState(
                    state.id.toString,
                    state.namespace,
                    state.project,
                    state.version,
                    state.job,
                    Phase.ofString(state.phase),
                    args.map(a => a.name -> a.value).toMap,
                    Status.ofString(state.status),
                    state.start_ts.map(_.toInstant.atZone(ZoneId.of("UTC"))),
                    state.end_ts.map(_.toInstant.atZone(ZoneId.of("UTC"))),
                    state.error
                )
            }
            .toSeq
            .sortWith(ordering)
    }

    def countJobs(query:JobQuery) : Int = {
        val q = queryJobs(query)
            .length

        Await.result(db.run(q.result), Duration.Inf)
    }

    def countJobs(query:JobQuery, grouping:JobColumn) : Seq[(String,Int)] = {
        def mapGroupingColumn(column:JobColumn) : JobRuns => Rep[String] = {
            column match {
                case JobColumn.DATETIME => t => t.start_ts.asColumnOf[String]
                case JobColumn.ID => t => t.id.asColumnOf[String]
                case JobColumn.PROJECT => t => t.project
                case JobColumn.NAME => t => t.job
                case JobColumn.PHASE => t => t.phase
                case JobColumn.STATUS => t => t.status
            }
        }

        val q = queryJobs(query)
            .groupBy(j => mapGroupingColumn(grouping)(j))
            .map(kv => kv._1 -> kv._2.length)

        Await.result(db.run(q.result), Duration.Inf)
    }

    def getTargetState(target:TargetRun, partitions:Map[String,String]) : Option[TargetState] = {
        val latestId = targetRuns
            .filter(tr => tr.namespace === target.namespace
                && tr.project === target.project
                && tr.target === target.target
                && tr.partitions_hash === target.partitions_hash
                && tr.status =!= Status.SKIPPED.toString
            )
            .map(_.id)
            .max

        // Finally select the run with the calculated ID
        val q = targetRuns.filter(r => r.id === latestId)
        val tgt = Await.result(db.run(q.result), Duration.Inf)
            .headOption

        // Retrieve target partitions
        val qp = tgt.map(t => targetPartitions.filter(_.target_id === t.id))
        val parts = qp.toSeq.flatMap(q =>
            Await.result(db.run(q.result), Duration.Inf)
                .map(a => (a.name, a.value))
        ).toMap

        tgt.map(state => TargetState(
            state.id.toString,
            state.job_id.map(_.toString),
            state.namespace,
            state.project,
            state.version,
            state.target,
            parts,
            Phase.ofString(state.phase),
            Status.ofString(state.status),
            state.start_ts.map(_.toInstant.atZone(ZoneId.of("UTC"))),
            state.end_ts.map(_.toInstant.atZone(ZoneId.of("UTC"))),
            state.error
        ))
    }

    def setTargetStatus(run:TargetRun) : Unit = {
        val q = targetRuns.filter(_.id === run.id).map(r => (r.end_ts, r.status, r.error)).update((run.end_ts, run.status, run.error))
        Await.result(db.run(q), Duration.Inf)
    }

    def insertTargetRun(run:TargetRun, partitions:Map[String,String]) : TargetRun = {
        val runQuery = (targetRuns returning targetRuns.map(_.id) into((run, id) => run.copy(id=id))) += run
        val runResult = Await.result(db.run(runQuery), Duration.Inf)

        val runPartitions = partitions.map(kv => TargetPartition(runResult.id, kv._1, kv._2))
        val argsQuery = targetPartitions ++= runPartitions
        Await.result(db.run(argsQuery), Duration.Inf)

        runResult
    }

    private def queryTargets(query:TargetQuery) : Query[TargetRuns, TargetRun, Seq] = {
        query.partitions.foldLeft(targetRuns.map(identity))((q, kv) => q
            .join(targetPartitions).on(_.id === _.target_id)
            .filter(a => a._2.name === kv._1 && a._2.value === kv._2)
            .map(xy => xy._1)
        )
            .optionalFilter(query.id)(_.id === _.toLong)
            .optionalFilter(query.namespace)(_.namespace === _)
            .optionalFilter(query.project)(_.project === _)
            .optionalFilter(query.target)(_.target === _)
            .optionalFilter(query.status)(_.status === _.toString)
            .optionalFilter(query.phase)(_.phase === _.toString)
            .optionalFilter2(query.jobId)((e,v) => e.job_id === v.toLong)
            .optionalFilter(query.from)((e,v) => e.start_ts.getOrElse(new Timestamp(0)) >= Timestamp.from(v.toInstant))
            .optionalFilter(query.to)((e,v) => e.start_ts.getOrElse(new Timestamp(0)) <= Timestamp.from(v.toInstant))
    }

    private def mapTargetColumn(column:TargetColumn) : TargetRuns => Rep[_] = {
        column match {
            case TargetColumn.DATETIME => t => t.start_ts
            case TargetColumn.ID => t => t.id
            case TargetColumn.PROJECT => t => t.project
            case TargetColumn.NAME => t => t.target
            case TargetColumn.PHASE => t => t.phase
            case TargetColumn.STATUS => t => t.status
            case TargetColumn.PARENT_ID => t => t.job_id
        }
    }


    def findTargets(query:TargetQuery, order:Seq[TargetOrder], limit:Int, offset:Int) : Seq[TargetState] = {
        def mapOrderDirection(order:TargetOrder) : slick.ast.Ordering = {
            if (order.isAscending)
                slick.ast.Ordering(slick.ast.Ordering.Asc)
            else
                slick.ast.Ordering(slick.ast.Ordering.Desc)
        }
        val ordering =
            (l:TargetState, r:TargetState) => {
                order.map { o =>
                    val result = o.column match {
                        case TargetColumn.DATETIME => cmpOpt(l.startDateTime, r.startDateTime)(cmpDt)
                        case TargetColumn.ID => cmpStr(l.id, r.id)
                        case TargetColumn.PROJECT => cmpStr(l.project, r.project)
                        case TargetColumn.NAME => cmpStr(l.target, r.target)
                        case TargetColumn.PHASE => cmpStr(l.phase.toString, r.phase.toString)
                        case TargetColumn.STATUS => cmpStr(l.status.toString, r.status.toString)
                        case TargetColumn.PARENT_ID => cmpOpt(l.jobId, r.jobId)(cmpStr)
                    }
                    if (o.isAscending)
                        result
                    else
                        -result
                }
                .find(_ != 0)
                .exists(_ < 0)
            }

        val q = queryTargets(query)
            .sorted(job => new slick.lifted.Ordered(order.map(o => (mapTargetColumn(o.column)(job).toNode, mapOrderDirection(o))).toVector))
            .drop(offset)
            .take(limit)
            .joinLeft(targetPartitions).on(_.id === _.target_id)

        Await.result(db.run(q.result), Duration.Inf)
            .groupBy(_._1.id)
            .values
            .map { states =>
                val state = states.head._1
                val partitions = states.flatMap(_._2)
                TargetState(
                    state.id.toString,
                    state.job_id.map(_.toString),
                    state.namespace,
                    state.project,
                    state.version,
                    state.target,
                    partitions.map(t => t.name -> t.value).toMap,
                    Phase.ofString(state.phase),
                    Status.ofString(state.status),
                    state.start_ts.map(_.toInstant.atZone(ZoneId.of("UTC"))),
                    state.end_ts.map(_.toInstant.atZone(ZoneId.of("UTC"))),
                    state.error
                )
            }
            .toSeq
            .sortWith(ordering)
    }

    def countTargets(query:TargetQuery) : Int = {
        val q = queryTargets(query)
            .length

        Await.result(db.run(q.result), Duration.Inf)
    }

    def countTargets(query:TargetQuery, grouping:TargetColumn) : Seq[(String,Int)] = {
        def mapGroupingColumn(column:TargetColumn) : TargetRuns => Rep[String] = {
            column match {
                case TargetColumn.DATETIME => t => t.start_ts.asColumnOf[String]
                case TargetColumn.ID => t => t.id.asColumnOf[String]
                case TargetColumn.PROJECT => t => t.project
                case TargetColumn.NAME => t => t.target
                case TargetColumn.PHASE => t => t.phase
                case TargetColumn.STATUS => t => t.status
                case TargetColumn.PARENT_ID => t => t.job_id.asColumnOf[String]
            }
        }

        val q = queryTargets(query)
            .groupBy(j => mapGroupingColumn(grouping)(j))
            .map(kv => kv._1 -> kv._2.length)

        Await.result(db.run(q.result), Duration.Inf)
    }

    private def cmpOpt[T](l:Option[T],r:Option[T])(lt:(T,T) => Int) : Int = {
        if (l.isEmpty && r.isEmpty)
            0
        else if (l.isEmpty)
            -1
        else if (r.isEmpty)
            1
        else
            lt(l.get, r.get)
    }
    private def cmpStr(l:String, r:String) : Int = {
        if (l < r)
            -1
        else if (l == r)
            0
        else
            1
    }
    private def cmpDt(l:ZonedDateTime, r:ZonedDateTime) : Int = {
        if (l == r)
            0
        else if (l.isBefore(r))
            -1
        else
            1
    }
}
