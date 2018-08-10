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

package com.dimajix.flowman.spec.model

import java.sql.Connection
import java.util.Properties

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.execution.datasources.jdbc.{JdbcUtils => SparkJdbcUtils}
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.ConnectionIdentifier
import com.dimajix.flowman.spec.connection.JdbcConnection
import com.dimajix.flowman.types.FieldValue
import com.dimajix.flowman.types.SingleValue
import com.dimajix.flowman.util.JdbcUtils
import com.dimajix.flowman.util.SchemaUtils


object JdbcRelation {
    private val logger = LoggerFactory.getLogger(classOf[JdbcRelation])

    private class JDBCSink(url: String, parameters: Map[String, String], rddSchema:StructType) extends org.apache.spark.sql.ForeachWriter[org.apache.spark.sql.Row] {
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
}


class JdbcRelation extends SchemaRelation {
    import com.dimajix.flowman.spec.model.JdbcRelation.JDBCSink

    private val logger = LoggerFactory.getLogger(classOf[JdbcRelation])

    @JsonProperty(value="connection") private var _connection: String = _
    @JsonProperty(value="properties") private var _properties:Map[String,String] = Map()
    @JsonProperty(value="database") private var _database: String = _
    @JsonProperty(value="table") private var _table: String = _

    def connection(implicit context: Context) : ConnectionIdentifier = ConnectionIdentifier.parse(context.evaluate(_connection))
    def properties(implicit context: Context) : Map[String,String] = _properties.mapValues(context.evaluate)
    def database(implicit context:Context) : String = context.evaluate(_database)
    def table(implicit context:Context) : String = context.evaluate(_table)

    /**
      * Reads the configured table from the source
      * @param executor
      * @param schema
      * @return
      */
    override def read(executor:Executor, schema:StructType, partitions:Map[String,FieldValue] = Map()) : DataFrame = {
        implicit val context = executor.context

        val tableName = Option(database).filter(_.nonEmpty).map(_ + ".").getOrElse("") + table

        logger.info(s"Reading data from JDBC source '$tableName' in database '$connection'")

        // Get Connection
        val (url,props) = createProperties(context)

        // Connect to database
        val reader = this.reader(executor)
        val df = reader.jdbc(url, tableName, props)
        SchemaUtils.applySchema(df, schema)
    }

    /**
      * Writes a given DataFrame into a JDBC connection
      *
      * @param executor
      * @param df
      * @param partition
      * @param mode
      */
    override def write(executor:Executor, df:DataFrame, partition:Map[String,SingleValue], mode:String) : Unit = {
        implicit val context = executor.context

        val tableName = Option(database).filter(_.nonEmpty).map(_ + ".").getOrElse("") + table

        logger.info(s"Writing data to JDBC source '$tableName' in database '$connection'")

        val (url,props) = createProperties(context)
        this.writer(executor, df)
            .jdbc(url, tableName, props)
    }


    /**
      * Writes data to a streaming sink
      *
      * @param executor
      * @param df
      * @return
      */
    override def writeStream(executor: Executor, df: DataFrame, mode: OutputMode, checkpointLocation: Path): StreamingQuery = {
        implicit val context = executor.context
        val tableName = Option(database).filter(_.nonEmpty).map(_ + ".").getOrElse("") + table
        logger.info(s"Writing data to JDBC source '$tableName' in database '$connection'")

        val (url,props) = createProperties(context)

        val writer = new JDBCSink(url, props.asScala.toMap, df.schema)
        this.streamWriter(executor, df, mode, checkpointLocation)
            .foreach(writer)
            .start()
    }

    override def create(executor:Executor) : Unit = ???
    override def destroy(executor:Executor) : Unit = ???
    override def migrate(executor:Executor) : Unit = ???

    private def createProperties(implicit context: Context) = {
        // Get Connection
        val db = context.getConnection(connection).asInstanceOf[JdbcConnection]
        val props = new Properties()
        props.setProperty("user", db.username)
        props.setProperty("password", db.password)
        props.setProperty("driver", db.driver)

        db.properties.foreach(kv => props.setProperty(kv._1, kv._2))
        properties.foreach(kv => props.setProperty(kv._1, kv._2))

        logger.info("Connecting to jdbc source at {}", db.url)

        (db.url,props)
    }
}
