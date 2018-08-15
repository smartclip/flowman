/*
 * Copyright 2018 Kaya Kupferschmidt
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

package com.dimajix.spark.sources.jdbc

import java.sql.Connection

import scala.util.control.NonFatal

import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.execution.datasources.jdbc.{JdbcUtils => SparkJdbcUtils}
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

import com.dimajix.flowman.spec.model.JdbcRelation.logger


object JdbcSink {
    private val logger = LoggerFactory.getLogger(classOf[JdbcSink])
}


class JdbcSink(url: String, parameters: Map[String, String], rddSchema:StructType) extends org.apache.spark.sql.ForeachWriter[org.apache.spark.sql.Row] {
    import JdbcSink.logger

    private val options = new JDBCOptions(parameters)
    private val batchSize = options.batchSize
    private val dialect = JdbcDialects.get(url)
    @transient private lazy val factory = SparkJdbcUtils.createConnectionFactory(options)
    private val tableSchema = determineTableSchema()
    private val statement = SparkJdbcUtils.getInsertStatement(options.table, rddSchema, tableSchema, false, dialect)
    private val isolationLevel = determineIsolationLevel()
    private val supportsTransactions = isolationLevel != Connection.TRANSACTION_NONE

    @transient private var conn:java.sql.Connection = _
    @transient private var stmt:java.sql.PreparedStatement = _
    @transient private var setters:Seq[JdbcUtils.JDBCValueSetter] = Seq()
    @transient private var nullTypes:Seq[Int] = Seq()
    @transient private var rowCount = 0

    def open(partitionId: Long, version: Long):Boolean = {
        conn = factory()
        if (supportsTransactions) {
            conn.setAutoCommit(false) // Everything in the same db transaction.
            conn.setTransactionIsolation(isolationLevel)
        }
        stmt = conn.prepareStatement(statement)
        setters = rddSchema.fields.map(f => JdbcUtils.getSetter(conn, dialect, f.dataType))
        nullTypes = rddSchema.fields.map(f => JdbcUtils.getJdbcType(dialect, f.dataType).jdbcNullType)
        rowCount = 0
        true
    }

    def process(row: org.apache.spark.sql.Row): Unit = {
        try {
            val numFields = rddSchema.fields.length

            try {
                // stmt.setQueryTimeout(options.queryTimeout)

                var i = 0
                while (i < numFields) {
                    if (row.isNullAt(i)) {
                        stmt.setNull(i + 1, nullTypes(i))
                    } else {
                        setters(i).apply(stmt, row, i)
                    }
                    i = i + 1
                }
                stmt.addBatch()
                rowCount += 1
                if (rowCount % batchSize == 0) {
                    stmt.executeBatch()
                    rowCount = 0
                }
            } finally {
                stmt.close()
            }
        }
        catch {
            case t:Exception => logger.error("Caught exception while writing to JDBC database", t)
        }
    }

    def close(errorOrNull:Throwable):Unit = {
        if (rowCount > 0) {
            stmt.executeBatch()
        }
        if (supportsTransactions) {
            conn.commit()
        }

        try {
            conn.close()
        } catch {
            case t:Exception => logger.error("Caught exception while closing JDBC connection", t)
        }
        conn = null
    }

    private def determineTableSchema() : Option[StructType] = {
        val con = factory()
        val result = SparkJdbcUtils.getSchemaOption(con, options)
        con.close()
        result
    }

    private def determineIsolationLevel() : Int = {
        if (options.isolationLevel != Connection.TRANSACTION_NONE) {
            val conn = factory()
            try {
                val metadata = conn.getMetaData
                if (metadata.supportsTransactions()) {
                    // Update to at least use the default isolation, if any transaction level
                    // has been chosen and transactions are supported
                    val defaultIsolation = metadata.getDefaultTransactionIsolation
                    if (metadata.supportsTransactionIsolationLevel(options.isolationLevel)) {
                        // Finally update to actually requested level if possible
                        options.isolationLevel
                    } else {
                        logger.warn(s"Requested isolation level ${options.isolationLevel} is not supported; " +
                            s"falling back to default isolation level $defaultIsolation")
                        defaultIsolation
                    }
                }
                else {
                    logger.warn(s"Requested isolation level ${options.isolationLevel}, but transactions are unsupported")
                    Connection.TRANSACTION_NONE
                }
            } catch {
                case NonFatal(e) =>
                    logger.warn("Exception while detecting transaction support", e)
                    Connection.TRANSACTION_NONE
            }
            finally {
                conn.close()
            }
        }
        else {
            Connection.TRANSACTION_NONE
        }
    }
}
