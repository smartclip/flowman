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
import java.sql.PreparedStatement
import java.util.Locale

import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils.getCommonJDBCType
import org.apache.spark.sql.jdbc.JdbcDialect
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.jdbc.JdbcType
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
import org.apache.spark.sql.types.TimestampType

import com.dimajix.flowman.types.Field


object JdbcUtils {
    type JDBCValueSetter = (PreparedStatement, Row, Int) => Unit

    /**
      * Compute the schema string for this RDD.
      */
    def getSchemaString(schema:Seq[Field], url: String) : String = {
        val sb = new StringBuilder()
        val dialect = JdbcDialects.get(url)
        schema.foreach { field =>
            val name = dialect.quoteIdentifier(field.name)
            val typ = getJdbcType(dialect, field.sparkType).databaseTypeDefinition
            val nullable = if (field.nullable) "" else "NOT NULL"
            val default = Option(field.default).filter(_.nonEmpty).map(d => s"DEFAULT '$d'").getOrElse("")
            val comment = Option(field.description).filter(_.nonEmpty).map(d => s"COMMENT '$d'").getOrElse("")
            sb.append(s", $name $typ $nullable $default $comment")
        }
        if (sb.length < 2) "" else sb.substring(2)
    }

    /**
      * Creates a table with a given schema.
      */
    def createTable(conn: Connection, fields:Seq[Field], options: JDBCOptions): Unit = {
        val strSchema = getSchemaString(fields, options.url)
        val table = options.table
        val createTableOptions = options.createTableOptions
        // Create the table if the table does not exist.
        // To allow certain options to append when create a new table, which can be
        // table_options or partition_options.
        // E.g., "CREATE TABLE t (name string) ENGINE=InnoDB DEFAULT CHARSET=utf8"
        val sql = s"CREATE TABLE $table ($strSchema) $createTableOptions"
        val statement = conn.createStatement
        try {
            //statement.setQueryTimeout(options.queryTimeout)
            statement.executeUpdate(sql)
        } finally {
            statement.close()
        }
    }

    def getJdbcType(dialect: JdbcDialect, dt: DataType): JdbcType = {
        dialect.getJDBCType(dt).orElse(getCommonJDBCType(dt)).getOrElse(
            throw new IllegalArgumentException(s"Can't get JDBC type for ${dt.catalogString}"))
    }

    def getSetter(conn: Connection,
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
            val typeName = getJdbcType(dialect, et).databaseTypeDefinition
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

}
