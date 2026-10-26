/*
 * Copyright (C) 2018 The Flowman Authors
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

package com.dimajix.flowman.catalog

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.analysis.DatabaseAlreadyExistsException
import org.apache.spark.sql.catalyst.analysis.NoSuchDatabaseException
import org.apache.spark.sql.catalyst.analysis.NoSuchPartitionException
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException

import com.dimajix.flowman.model.PartitionSchema
import com.dimajix.flowman.types.Field


/**
  * This trait abstracts away the details of the underlying table catalog. The catalog is used for managing Hive
  * table instances (and tables in external catalogs like Impala). The catalog provides operations for creating,
  * altering and dropping tables, views and partitions.
  *
  * Note that the public signatures of this trait still expose some Spark data types ([[CatalogTable]],
  * [[CatalogTablePartition]], [[StructType]]). These are gradually being replaced by Flowman data types
  * ([[TableDefinition]]) to make the trait usable from a Spark Connect client.
  */
trait Catalog {
    /**
      * Returns the current database of the underlying Spark session
      */
    def currentDatabase : String

    /**
      * Lists all (external) databases
      * @return
      */
    def listDatabases() : Seq[String]

    /**
      * Creates a new database
      * @param database
      * @param catalog
      * @param location
      * @param ignoreIfExists
      */
    @throws[DatabaseAlreadyExistsException]
    def createDatabase(database:String, catalog:String, location:Option[Path], ignoreIfExists:Boolean) : Unit

    /**
      * Returns true if the specified database exists
      * @param database
      * @return
      */
    def databaseExists(database:String) : Boolean

    /**
      * Drops an existing database
      * @param database
      * @param ignoreIfNotExists
      */
    @throws[NoSuchDatabaseException]
    def dropDatabase(database:String, ignoreIfNotExists:Boolean) : Unit

    /**
      * Lists all tables in an external database
      * @param database
      * @return
      */
    @throws[NoSuchDatabaseException]
    def listTables(database:String) : Seq[TableIdentifier]

    /**
      * Lists all tables matching a specific name pattern in an external database
      * @param database
      * @param pattern
      * @return
      */
    @throws[NoSuchDatabaseException]
    def listTables(database:String, pattern:String) : Seq[TableIdentifier]

    /**
      * Creates a new table from a detailed definition
      * @param table
      * @param ignoreIfExists
      */
    @throws[NoSuchDatabaseException]
    @throws[TableAlreadyExistsException]
    def createTable(table:TableDefinition, ignoreIfExists:Boolean) : Unit

    /**
      * Refreshes any meta information about a table. This might be required either when the schema changes
      * or when new data is written into a table
      * @param table
      */
    @throws[NoSuchDatabaseException]
    @throws[NoSuchTableException]
    def refreshTable(table:TableIdentifier) : Unit

    /**
      * Returns true if the specified Hive table or view actually exists
      * @param name
      * @return
      */
    def tableExists(name:TableIdentifier) : Boolean

    /**
      * Returns information about a Hive table or view
      * @param name
      * @return
      */
    @throws[NoSuchDatabaseException]
    @throws[NoSuchTableException]
    def getTable(name:TableIdentifier) : TableDefinition

    /**
      * Returns the storage location of a Hive table
      * @param name
      * @return
      */
    @throws[NoSuchDatabaseException]
    @throws[NoSuchTableException]
    def getTableLocation(name:TableIdentifier) : Path

    /**
      * Drops a whole table including all partitions and all files
      * @param table
      * @param ignoreIfNotExists
      */
    @throws[NoSuchDatabaseException]
    @throws[NoSuchTableException]
    def dropTable(table:TableIdentifier, ignoreIfNotExists:Boolean=false) : Unit

    /**
      * Truncates a table by either removing the corresponding file or by dropping all partitions
      * @param table
      */
    @throws[NoSuchDatabaseException]
    @throws[NoSuchTableException]
    def truncateTable(table:TableIdentifier) : Unit

    /**
      * Applies a list of [[TableChange]] modifications to an existing Hive table
      * @param table
      * @param changes
      */
    @throws[NoSuchDatabaseException]
    @throws[NoSuchTableException]
    def alterTable(table:TableIdentifier, changes:Seq[TableChange]) : Unit

    /**
      * Adds one or more new columns to an existing Hive table
      * @param table
      * @param colsToAdd
      */
    @throws[NoSuchDatabaseException]
    @throws[NoSuchTableException]
    def addTableColumns(table:TableIdentifier, colsToAdd:Seq[Field]) : Unit

    /**
      * Checks if the given partition exists
      * @param table
      * @param partition
      * @return
      */
    @throws[NoSuchDatabaseException]
    @throws[NoSuchTableException]
    def partitionExists(table:TableIdentifier, partition:PartitionSpec) : Boolean

    /**
      * Retrieve the metadata of a table partition, assuming it exists
      * @param table
      * @param partition
      * @return
      */
    @throws[NoSuchDatabaseException]
    @throws[NoSuchTableException]
    @throws[NoSuchPartitionException]
    def getPartition(table:TableIdentifier, partition:PartitionSpec) : PartitionDefinition

    /**
      * Returns the storage location of a single partition of a Hive table
      * @param table
      * @param partition
      * @return
      */
    @throws[NoSuchDatabaseException]
    @throws[NoSuchTableException]
    @throws[NoSuchPartitionException]
    def getPartitionLocation(table:TableIdentifier, partition:PartitionSpec) : Path

    /**
      * Returns the partition schema of a Hive table
      * @param table
      * @return
      */
    @throws[NoSuchDatabaseException]
    @throws[NoSuchTableException]
    def getPartitionSchema(table:TableIdentifier) : PartitionSchema

    /**
      * Adds a new partition to an existing table (if it does not yet exist)
      * @param table
      * @param partition
      * @param location
      */
    @throws[NoSuchDatabaseException]
    @throws[NoSuchTableException]
    def addPartition(table:TableIdentifier, partition:PartitionSpec, location:Path) : Unit

    /**
      * Adds or replaces a partition to an existing table
      * @param table
      * @param partition
      * @param location
      */
    @throws[NoSuchDatabaseException]
    @throws[NoSuchTableException]
    def addOrReplacePartition(table:TableIdentifier, partition:PartitionSpec, location:Path) : Unit

    /**
      * Refreshes the metadata of a specific partition of a Hive table
      * @param table
      * @param partition
      */
    @throws[NoSuchDatabaseException]
    @throws[NoSuchTableException]
    def refreshPartition(table:TableIdentifier, partition:PartitionSpec) : Unit

    /**
      * Removes all data of a specifc partition
      * @param table
      * @param partition
      */
    @throws[NoSuchDatabaseException]
    @throws[NoSuchTableException]
    def truncatePartition(table:TableIdentifier, partition:PartitionSpec) : Unit

    /**
      * Drops a whole partition
      * @param table
      * @param partition
      * @param ignoreIfNotExists
      */
    @throws[NoSuchDatabaseException]
    @throws[NoSuchTableException]
    def dropPartition(table:TableIdentifier, partition:PartitionSpec, ignoreIfNotExists:Boolean=false) : Unit

    /**
      * Drops a list of partitions
      * @param table
      * @param partitions
      * @param ignoreIfNotExists
      */
    @throws[NoSuchDatabaseException]
    @throws[NoSuchTableException]
    def dropPartitions(table:TableIdentifier, partitions:Seq[PartitionSpec], ignoreIfNotExists:Boolean=false) : Unit

    /**
      * Creates a new view
      * @param table
      * @param select
      * @param ignoreIfExists
      */
    @throws[NoSuchDatabaseException]
    @throws[TableAlreadyExistsException]
    def createView(table:TableIdentifier, select:String, ignoreIfExists:Boolean) : Unit

    /**
      * Alters an existing view
      * @param table
      * @param select
      */
    @throws[NoSuchDatabaseException]
    @throws[NoSuchTableException]
    def alterView(table:TableIdentifier, select:String) : Unit

    /**
      * Drops a view
      * @param table
      * @param ignoreIfNotExists
      */
    @throws[NoSuchDatabaseException]
    @throws[NoSuchTableException]
    def dropView(table:TableIdentifier, ignoreIfNotExists:Boolean=false) : Unit
}
