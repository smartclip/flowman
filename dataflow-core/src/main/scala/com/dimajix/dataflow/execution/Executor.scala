package com.dimajix.dataflow.execution

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

import com.dimajix.dataflow.spec.TableIdentifier


abstract class Executor {
    /**
      * Returns (or lazily creates) a SparkSession of this Executor. The SparkSession will be derived from the global
      * SparkSession, but a new derived session with a separate namespace will be created.
      *
      * @return
      */
    def session: SparkSession

    /**
      * Returns the Context associated with this Executor. This context will be used for looking up
      * databases and relations and for variable substition
      *
      * @return
      */
    def context : Context

    /**
      * Returns a map of all temporary tables created by this executor
      *
      * @return
      */
    def tables : Map[TableIdentifier,DataFrame]

    /**
      * Returns a named table created by an executor. If a project is specified, Executors for other projects
      * will be searched as well
      *
      * @param identifier
      * @return
      */
    def getTable(identifier: TableIdentifier) : DataFrame

    /**
      * Creates an instance of a table of a Dataflow, or retrieves it from cache
      *
      * @param identifier
      */
    def instantiate(identifier: TableIdentifier) : DataFrame

    def cleanup() : Unit
}
