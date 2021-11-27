/*
 * Copyright 2018-2021 Kaya Kupferschmidt
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

import java.security.MessageDigest
import java.sql.SQLRecoverableException
import java.sql.SQLTransientException
import java.sql.Timestamp
import java.time.Clock
import java.time.ZoneId

import javax.xml.bind.DatatypeConverter
import org.slf4j.LoggerFactory
import slick.jdbc.DerbyProfile
import slick.jdbc.H2Profile
import slick.jdbc.MySQLProfile
import slick.jdbc.PostgresProfile
import slick.jdbc.SQLServerProfile
import slick.jdbc.SQLiteProfile

import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.Status
import com.dimajix.flowman.graph.GraphBuilder
import com.dimajix.flowman.history.JdbcStateRepository.JobRun
import com.dimajix.flowman.history.JdbcStateRepository.TargetRun
import com.dimajix.flowman.history.JdbcStateStore.JdbcJobToken
import com.dimajix.flowman.history.JdbcStateStore.JdbcTargetToken
import com.dimajix.flowman.metric.GaugeMetric
import com.dimajix.flowman.metric.Metric
import com.dimajix.flowman.model.Job
import com.dimajix.flowman.model.JobDigest
import com.dimajix.flowman.model.JobResult
import com.dimajix.flowman.model.Target
import com.dimajix.flowman.model.TargetDigest
import com.dimajix.flowman.model.TargetResult



object JdbcStateStore {
    case class Connection(
        url:String,
        driver:String,
        user:Option[String] = None,
        password:Option[String] = None,
        properties: Map[String,String] = Map()
    )

    case class JdbcTargetToken(
        run:TargetRun,
        parent:Option[JdbcJobToken]
    ) extends TargetToken

    case class JdbcJobToken(
        run:JobRun,
        graph:GraphBuilder
    ) extends JobToken
}


case class JdbcStateStore(connection:JdbcStateStore.Connection, retries:Int=3, timeout:Int=1000) extends StateStore {
    import JdbcStateRepository._

    private val logger = LoggerFactory.getLogger(classOf[JdbcStateStore])

    /**
      * Returns the state of a job, or None if no information is available
      * @param job
      * @return
      */
    override def getJobState(job: JobDigest): Option[JobState] = {
        val run = JobRun(
            0,
            job.namespace,
            job.project,
            null,
            job.job,
            null,
            hashArgs(job),
            None,
            None,
            null,
            None
        )
        logger.debug(s"Checking last state of '${run.phase}' job '${run.namespace}/${run.project}/${run.job}' in history database")
        withSession { repository =>
            repository.getJobState(run)
        }
    }

    /**
     * Returns all metrics belonging to a specific job instance
     * @param jobId
     * @return
     */
    override def getJobMetrics(jobId:String) : Seq[Measurement] = {
        withSession { repository =>
            repository.getJobMetrics(jobId.toLong)
        }
    }

    /**
     * Returns the execution graph belonging to a specific job run
     *
     * @param jobId
     * @return
     */
    override def getJobGraph(jobId: String): Option[Graph] = {
        withSession { repository =>
            repository.getJobGraph(jobId.toLong)
        }
    }

    /**
      * Starts the run and returns a token, which can be anything
     *
     * @param digest
      * @return
      */
    override def startJob(job:Job, digest:JobDigest) : JobToken = {
        val now = new Timestamp(Clock.systemDefaultZone().instant().toEpochMilli)
        val run =  JobRun(
            0,
            job.namespace.map(_.name).getOrElse(""),
            job.project.map(_.name).getOrElse(""),
            job.project.flatMap(_.version).getOrElse(""),
            job.name,
            digest.phase.upper,
            hashArgs(digest),
            Some(now),
            None,
            Status.RUNNING.upper,
            None
        )

        logger.debug(s"Start '${digest.phase}' job '${run.namespace}/${run.project}/${run.job}' in history database")
        val run2 = withSession { repository =>
            repository.insertJobRun(run, digest.args)
        }

        JdbcJobToken(run2, new GraphBuilder(job.context, digest.phase))
    }

    /**
      * Marks a run as a success
      *
      * @param token
      */
    override def finishJob(token:JobToken, result: JobResult, metrics:Seq[Measurement]=Seq()) : Unit = {
        val status = result.status
        val jdbcToken = token.asInstanceOf[JdbcJobToken]
        val run = jdbcToken.run
        logger.info(s"Mark '${run.phase}' job '${run.namespace}/${run.project}/${run.job}' as $status in history database")

        val now = new Timestamp(Clock.systemDefaultZone().instant().toEpochMilli)
        val graph = Graph.ofGraph(jdbcToken.graph.build())
        withSession{ repository =>
            repository.setJobStatus(run.copy(end_ts = Some(now), status=status.upper, error=result.exception.map(_.toString)))
            repository.insertJobMetrics(run, metrics)
            repository.insertJobGraph(run, graph)
        }
    }

    /**
      * Returns the state of a specific target on its last run, or None if no information is available
      * @param target
      * @return
      */
    override def getTargetState(target:TargetDigest) : Option[TargetState] = {
        val run = TargetRun(
            0,
            None,
            target.namespace,
            target.project,
            null,
            target.target,
            null,
            hashPartitions(target),
            None,
            None,
            null,
            None
        )
        logger.debug(s"Checking state of target '${run.namespace}/${run.project}/${run.target}' in history database")
        withSession { repository =>
            repository.getTargetState(run, target.partitions)
        }
    }

    def getTargetState(targetId: String): TargetState = {
        withSession { repository =>
            repository.getTargetState(targetId.toLong)
        }
    }

    /**
      * Starts the run and returns a token, which can be anything
      * @param digest
      * @return
      */
    override def startTarget(target:Target, digest:TargetDigest, parent:Option[JobToken]) : TargetToken = {
        val now = new Timestamp(Clock.systemDefaultZone().instant().toEpochMilli)

        val parentRun = parent.map(_.asInstanceOf[JdbcJobToken])
        val run =  TargetRun(
            0,
            parentRun.map(_.run.id),
            target.namespace.map(_.name).getOrElse(""),
            target.project.map(_.name).getOrElse(""),
            target.project.flatMap(_.version).getOrElse(""),
            target.name,
            digest.phase.upper,
            hashPartitions(digest),
            Some(now),
            None,
            Status.RUNNING.upper,
            None
        )

        logger.debug(s"Start '${digest.phase}' target '${run.namespace}/${run.project}/${run.target}' in history database")
        val run2 = withSession { repository =>
            repository.insertTargetRun(run, digest.partitions)
        }
        JdbcTargetToken(run2, parentRun)
    }

    /**
      * Marks a run as a success
      *
      * @param token
      */
    override def finishTarget(token:TargetToken, result: TargetResult) : Unit = {
        val status = result.status
        val jdbcToken = token.asInstanceOf[JdbcTargetToken]
        val run = jdbcToken.run
        logger.info(s"Mark '${run.phase}' target '${run.namespace}/${run.project}/${run.target}' as $status in history database")

        val now = new Timestamp(Clock.systemDefaultZone().instant().toEpochMilli)
        withSession{ repository =>
            repository.setTargetStatus(run.copy(end_ts = Some(now), status=status.upper, error=result.exception.map(_.toString)))
        }

        // Add target to Job's build graph
        if (status != Status.SKIPPED) {
            jdbcToken.parent.foreach {
                _.graph.addTarget(result.target)
            }
        }
    }

    /**
      * Returns a list of job matching the query criteria
      * @param query
      * @param limit
      * @param offset
      * @return
      */
    override def findJobs(query:JobQuery, order:Seq[JobOrder]=Seq(), limit:Int=10000, offset:Int=0) : Seq[JobState] = {
        withSession { repository =>
            repository.findJobs(query, order, limit, offset)
        }
    }


    override def countJobs(query: JobQuery): Int = {
        withSession { repository =>
            repository.countJobs(query)
        }
    }


    override def countJobs(query: JobQuery, grouping: JobColumn): Map[String, Int] = {
        withSession { repository =>
            repository.countJobs(query, grouping).toMap
        }
    }

    /**
      * Returns a list of job matching the query criteria
     *
     * @param query
      * @param limit
      * @param offset
      * @return
      */
    override def findTargets(query:TargetQuery, order:Seq[TargetOrder]=Seq(), limit:Int=10000, offset:Int=0) : Seq[TargetState] = {
        withSession { repository =>
            repository.findTargets(query, order, limit, offset)
        }
    }

    override def countTargets(query: TargetQuery): Int = {
        withSession { repository =>
            repository.countTargets(query)
        }
    }

    override def countTargets(query: TargetQuery, grouping: TargetColumn): Map[String, Int] = {
        withSession { repository =>
            repository.countTargets(query, grouping).toMap
        }
    }

    override def findJobMetrics(jobQuery: JobQuery, groupings: Seq[String]): Seq[MetricSeries] = {
        withSession { repository =>
            repository.findMetrics(jobQuery, groupings)
        }
    }

    private def hashArgs(job:JobDigest) : String = {
         hashMap(job.args)
    }

    private def hashPartitions(target:TargetDigest) : String = {
        hashMap(target.partitions)
    }

    private def hashMap(map:Map[String,String]) : String = {
        val strArgs = map.map(kv => kv._1 + "=" + kv._2).mkString(",")
        val bytes = strArgs.getBytes("UTF-8")
        val digest = MessageDigest.getInstance("MD5").digest(bytes)
        DatatypeConverter.printHexBinary(digest).toUpperCase()
    }

    /**
      * Performs some a task with a JDBC session, also automatically performing retries and timeouts
      *
      * @param query
      * @tparam T
      * @return
      */
    private def withSession[T](query: JdbcStateRepository => T) : T = {
        def retry[T](n:Int)(fn: => T) : T = {
            try {
                fn
            } catch {
                case e @(_:SQLRecoverableException|_:SQLTransientException) if n > 1 => {
                    logger.error("Retrying after error while executing SQL: {}", e.getMessage)
                    Thread.sleep(timeout)
                    retry(n - 1)(fn)
                }
            }
        }

        retry(retries) {
            val repository = newRepository()
            query(repository)
        }
    }

    private var tablesCreated:Boolean = false

    private def newRepository() : JdbcStateRepository = {
        // Get Connection
        val derbyPattern = """.*\.derby\..*""".r
        val sqlitePattern = """.*\.sqlite\..*""".r
        val h2Pattern = """.*\.h2\..*""".r
        val mariadbPattern = """.*\.mariadb\..*""".r
        val mysqlPattern = """.*\.mysql\..*""".r
        val postgresqlPattern = """.*\.postgresql\..*""".r
        val sqlserverPattern = """.*\.sqlserver\..*""".r
        val profile = connection.driver match {
            case derbyPattern() => DerbyProfile
            case sqlitePattern() => SQLiteProfile
            case h2Pattern() => H2Profile
            case mysqlPattern() => MySQLProfile
            case mariadbPattern() => MySQLProfile
            case postgresqlPattern() => PostgresProfile
            case sqlserverPattern() => SQLServerProfile
            case _ => throw new UnsupportedOperationException(s"Database with driver ${connection.driver} is not supported")
        }

        val repository = new JdbcStateRepository(connection, profile)

        // Create Database if not exists
        if (!tablesCreated) {
            repository.create()
            tablesCreated = true
        }

        repository
    }

}
