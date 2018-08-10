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
import java.sql.PreparedStatement
import java.util.Locale
import java.util.Properties

import scala.util.control.NonFatal
import scala.collection.JavaConverters._

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils.getCommonJDBCType
import org.apache.spark.sql.jdbc.JdbcDialect
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.jdbc.JdbcType
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.BinaryType
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.types.ByteType
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.DecimalType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.ShortType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.TimestampType
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.ConnectionIdentifier
import com.dimajix.flowman.spec.connection.JdbcConnection
import com.dimajix.flowman.spec.model.JdbcRelation.JDBCSink
import com.dimajix.flowman.types.FieldValue
import com.dimajix.flowman.types.SingleValue
import com.dimajix.flowman.util.SchemaUtils


object JdbcRelation {
    private val logger = LoggerFactory.getLogger(classOf[JdbcRelation])

    private class JDBCSink(url: String, parameters: Map[String, String], rddSchema:StructType) extends org.apache.spark.sql.ForeachWriter[org.apache.spark.sql.Row] {
        private type JDBCValueSetter = (PreparedStatement, Row, Int) => Unit

        val options = new JDBCOptions(parameters)
        val batchSize = options.batchSize
        val dialect = JdbcDialects.get(url)
        @transient lazy val factory = JdbcUtils.createConnectionFactory(options)
        val tableSchema = determineTableSchema()
        val statement = JdbcUtils.getInsertStatement(options.table, rddSchema, tableSchema, false, dialect)
        val isolationLevel = determineIsolationLevel()
        val supportsTransactions = isolationLevel != Connection.TRANSACTION_NONE

        @transient var conn:java.sql.Connection = _
        @transient var stmt:java.sql.PreparedStatement = _
        @transient var setters:Seq[JDBCValueSetter] = Seq()
        @transient var nullTypes:Seq[Int] = Seq()
        @transient var rowCount = 0

        def open(partitionId: Long, version: Long):Boolean = {
            conn = factory()
            if (supportsTransactions) {
                conn.setAutoCommit(false) // Everything in the same db transaction.
                conn.setTransactionIsolation(isolationLevel)
            }
            stmt = conn.prepareStatement(statement)
            setters = rddSchema.fields.map(f => makeSetter(conn, dialect, f.dataType))
            nullTypes = rddSchema.fields.map(f => getJdbcType(f.dataType, dialect).jdbcNullType)
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

        private def getJdbcType(dt: DataType, dialect: JdbcDialect): JdbcType = {
            dialect.getJDBCType(dt).orElse(getCommonJDBCType(dt)).getOrElse(
                throw new IllegalArgumentException(s"Can't get JDBC type for ${dt.catalogString}"))
        }

        private def makeSetter(conn: Connection,
                  dialect: JdbcDialect,
                  dataType: DataType): JDBCValueSetter = dataType match {
            case IntegerType =>
                (stmt: PreparedStatement, row: Row, pos: Int) =>
                    stmt.setInt(pos + 1, row.getInt(pos))

            case LongType =>
                (stmt: PreparedStatement, row: Row, pos: Int) =>
                    stmt.setLong(pos + 1, row.getLong(pos))

            case DoubleType =>
                (stmt: PreparedStatement, row: Row, pos: Int) =>
                    stmt.setDouble(pos + 1, row.getDouble(pos))

            case FloatType =>
                (stmt: PreparedStatement, row: Row, pos: Int) =>
                    stmt.setFloat(pos + 1, row.getFloat(pos))

            case ShortType =>
                (stmt: PreparedStatement, row: Row, pos: Int) =>
                    stmt.setInt(pos + 1, row.getShort(pos))

            case ByteType =>
                (stmt: PreparedStatement, row: Row, pos: Int) =>
                    stmt.setInt(pos + 1, row.getByte(pos))

            case BooleanType =>
                (stmt: PreparedStatement, row: Row, pos: Int) =>
                    stmt.setBoolean(pos + 1, row.getBoolean(pos))

            case StringType =>
                (stmt: PreparedStatement, row: Row, pos: Int) =>
                    stmt.setString(pos + 1, row.getString(pos))

            case BinaryType =>
                (stmt: PreparedStatement, row: Row, pos: Int) =>
                    stmt.setBytes(pos + 1, row.getAs[Array[Byte]](pos))

            case TimestampType =>
                (stmt: PreparedStatement, row: Row, pos: Int) =>
                    stmt.setTimestamp(pos + 1, row.getAs[java.sql.Timestamp](pos))

            case DateType =>
                (stmt: PreparedStatement, row: Row, pos: Int) =>
                    stmt.setDate(pos + 1, row.getAs[java.sql.Date](pos))

            case t: DecimalType =>
                (stmt: PreparedStatement, row: Row, pos: Int) =>
                    stmt.setBigDecimal(pos + 1, row.getDecimal(pos))

            case ArrayType(et, _) =>
                // remove type length parameters from end of type name
                val typeName = getJdbcType(et, dialect).databaseTypeDefinition
                    .toLowerCase(Locale.ROOT).split("\\(")(0)
                (stmt: PreparedStatement, row: Row, pos: Int) =>
                    val array = conn.createArrayOf(
                        typeName,
                        row.getSeq[AnyRef](pos).toArray)
                    stmt.setArray(pos + 1, array)

            case _ =>
                (_: PreparedStatement, _: Row, pos: Int) =>
                    throw new IllegalArgumentException(
                        s"Can't translate non-null value for field $pos")
        }

        private def determineTableSchema() : Option[StructType] = {
            val con = factory()
            val result = JdbcUtils.getSchemaOption(con, options)
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
