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

package com.dimajix.flowman.config

import java.io.File
import java.nio.file.FileSystem
import java.util.Locale
import java.util.NoSuchElementException

import org.apache.spark.SPARK_REPO_URL

import com.dimajix.flowman.SPARK_VERSION
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.execution.MigrationPolicy
import com.dimajix.flowman.execution.MigrationStrategy
import com.dimajix.flowman.execution.OutputMode
import com.dimajix.flowman.execution.SimpleExecutor
import com.dimajix.flowman.execution.DependencyScheduler
import com.dimajix.flowman.execution.Scheduler
import com.dimajix.flowman.model.VerifyPolicy
import com.dimajix.flowman.transforms.ColumnMismatchPolicy
import com.dimajix.flowman.transforms.CharVarcharPolicy
import com.dimajix.flowman.transforms.TypeMismatchPolicy
import com.dimajix.spark.features


object FlowmanConf {
    private val configEntries = java.util.Collections.synchronizedMap(
        new java.util.HashMap[String, ConfigEntry[_]]())

    private def register(entry: ConfigEntry[_]): Unit = configEntries.synchronized {
        require(!configEntries.containsKey(entry.key),
            s"Duplicate FlowmanConf entry. ${entry.key} has been registered")
        configEntries.put(entry.key, entry)
    }

    def buildConf(key: String): ConfigBuilder = ConfigBuilder(key).onCreate(register)

    val SPARK_EAGER_CACHE = buildConf("flowman.spark.eagerCache")
        .doc("Enables eager caching in Spark")
        .booleanConf
        .createWithDefault(false)
    val SPARK_ENABLE_HIVE = buildConf("flowman.spark.enableHive")
        .doc("Enables Hive support. WHen using newer Hadoop versions, you might want to disable it")
        .booleanConf
        .createWithDefault(features.hiveSupported)
    val HIVE_ANALYZE_TABLE = buildConf("flowman.hive.analyzeTable")
        .doc("Performs ANALYZE TABLE commands")
        .booleanConf
        .createWithDefault(true)
    val IMPALA_COMPUTE_STATS = buildConf("flowman.impala.computeStats")
        .doc("Performs COMPUTE STATS commands")
        .booleanConf
        .createWithDefault(true)
    val EXTERNAL_CATALOG_IGNORE_ERRORS = buildConf("flowman.externalCatalog.ignoreErrors")
        .doc("Ignore errors of external catalogs (like Impala)")
        .booleanConf
        .createWithDefault(false)

    val HOME_DIRECTORY = buildConf("flowman.home")
        .doc("Home directory of Flowman")
        .fileConf
        .createOptional
    val CONF_DIRECTORY = buildConf("flowman.conf.directory")
        .doc("Directory containing Flowman configuration")
        .fileConf
        .createOptional
    val PLUGIN_DIRECTORY = buildConf("flowman.plugin.directory")
        .doc("Directory containing Flowman plugins")
        .fileConf
        .createOptional

    val EXECUTION_TARGET_FORCE_DIRTY = buildConf("flowman.execution.target.forceDirty")
        .doc("Consider all targets as being 'dirty' without checking")
        .booleanConf
        .createWithDefault(false)
    val EXECUTION_TARGET_USE_HISTORY = buildConf("flowman.execution.target.useHistory")
        .doc("Consult history store for deciding if a target is dirty")
        .booleanConf
        .createWithDefault(false)
    val EXECUTION_EXECUTOR_CLASS = buildConf("flowman.execution.executor.class")
        .doc("Class name for executor used to run targets")
        .classConf(classOf[Executor])
        .createWithDefault(classOf[SimpleExecutor])
    val EXECUTION_EXECUTOR_PARALLELISM = buildConf("flowman.execution.executor.parallelism")
        .doc("Number of parallel targets to execute")
        .intConf
        .createWithDefault(4)
    val EXECUTION_SCHEDULER_CLASS = buildConf("flowman.execution.scheduler.class")
        .doc("Class name for scheduling targets")
        .classConf(classOf[Scheduler])
        .createWithDefault(classOf[DependencyScheduler])
    val EXECUTION_MAPPING_PARALLELISM = buildConf("flowman.execution.mapping.parallelism")
        .doc("Parallelism of mapping instantiation")
        .intConf
        .createWithDefault(1)
    val EXECUTION_MAPPING_SCHEMA_CACHE = buildConf("flowman.execution.mapping.schemaCache")
        .doc("Cache schema information of mapping instances")
        .booleanConf
        .createWithDefault(true)
    val EXECUTION_RELATION_SCHEMA_CACHE = buildConf("flowman.execution.relation.schemaCache")
        .doc("Cache schema information of relation instances")
        .booleanConf
        .createWithDefault(true)

    val DEFAULT_RELATION_MIGRATION_POLICY = buildConf("flowman.default.relation.migrationPolicy")
        .doc("Default migration policy. Allowed values are 'relaxed' and 'strict'")
        .stringConf
        .createWithDefault(MigrationPolicy.RELAXED.toString)
    val DEFAULT_RELATION_MIGRATION_STRATEGY = buildConf("flowman.default.relation.migrationStrategy")
        .doc("Default migration strategy. Allowed values are 'never', 'fail', 'alter', 'alter_replace' and 'replace'")
        .stringConf
        .createWithDefault(MigrationStrategy.ALTER.toString)

    val DEFAULT_RELATION_INPUT_COLUMN_MISMATCH_POLICY = buildConf("flowman.default.relation.input.columnMismatchPolicy")
        .doc("Default strategy to use on schema column mismatch while reading relations. Can be 'ignore', 'error', 'add_columns_or_ignore', 'add_columns_or_error', 'remove_columns_or_ignore', 'remove_columns_or_error', 'add_remove_columns'")
        .stringConf
        .createWithDefault(ColumnMismatchPolicy.IGNORE.toString)
    val DEFAULT_RELATION_INPUT_TYPE_MISMATCH_POLICY = buildConf("flowman.default.relation.input.typeMismatchPolicy")
        .doc("Default strategy to use on schema type mismatch while reading relations. Can be 'ignore', 'error', 'cast_compatible_or_ignore', 'cast_compatible_or_error', 'cast_always'")
        .stringConf
        .createWithDefault(TypeMismatchPolicy.IGNORE.toString)
    val DEFAULT_RELATION_INPUT_CHAR_VARCHAR_POLICY = buildConf("flowman.default.relation.input.charVarcharPolicy")
        .doc("Default strategy to use when reading CHAR(n)/VARCHAR(n) data types. Can be 'ignore', 'pad', 'truncate' or 'pad_and_truncate'")
        .stringConf
        .createWithDefault(CharVarcharPolicy.IGNORE.toString)
    val DEFAULT_RELATION_OUTPUT_COLUMN_MISMATCH_POLICY = buildConf("flowman.default.relation.output.columnMismatchPolicy")
        .doc("Default strategy to use on schema column mismatch while reading relations. Can be 'ignore', 'error', 'add_columns_or_ignore', 'add_columns_or_error', 'remove_columns_or_ignore', 'remove_columns_or_error', 'add_remove_columns'")
        .stringConf
        .createWithDefault(ColumnMismatchPolicy.ADD_REMOVE_COLUMNS.toString)
    val DEFAULT_RELATION_OUTPUT_TYPE_MISMATCH_POLICY = buildConf("flowman.default.relation.output.typeMismatchPolicy")
        .doc("Default strategy to use on schema type mismatch while reading relations. Can be 'ignore', 'error', 'cast_compatible_or_ignore', 'cast_compatible_or_error', 'cast_always'")
        .stringConf
        .createWithDefault(TypeMismatchPolicy.CAST_ALWAYS.toString)
    val DEFAULT_RELATION_OUTPUT_CHAR_VARCHAR_POLICY = buildConf("flowman.default.relation.output.charVarcharPolicy")
        .doc("Default strategy to use when writing CHAR(n)/VARCHAR(n) data types. Can be 'ignore', 'pad', 'truncate' or 'pad_and_truncate'")
        .stringConf
        .createWithDefault(CharVarcharPolicy.PAD_AND_TRUNCATE.toString)

    val DEFAULT_TARGET_VERIFY_POLICY = buildConf("flowman.default.target.verifyPolicy")
        .doc("Policy for verifying a target. Accepted verify policies are 'empty_as_success', 'empty_as_failure' and 'empty_as_success_with_errors'.")
        .stringConf
        .createWithDefault(VerifyPolicy.EMPTY_AS_FAILURE.toString)
    val DEFAULT_TARGET_OUTPUT_MODE = buildConf("flowman.default.target.outputMode")
        .doc("Default output mode of targets")
        .stringConf
        .createWithDefault(OutputMode.OVERWRITE.toString)
    val DEFAULT_TARGET_REBALANCE = buildConf("flowman.default.target.rebalance")
        .doc("Rebalances all outputs before writing")
        .booleanConf
        .createWithDefault(false)
    val DEFAULT_TARGET_PARALLELISM = buildConf("flowman.default.target.parallelism")
        .doc("Uses the specified number of partitions for writing targets. -1 disables")
        .intConf
        .createWithDefault(16)

    val WORKAROUND_ANALYZE_PARTITION = buildConf("flowman.workaround.analyze_partition")
        .doc("Enables workaround to setup a new HMS connection for ANALYZE PARTITION. Required for CDP 7.1")
        .booleanConf
        .createWithDefault(SPARK_VERSION.matches("\\d.\\d.\\d.7.\\d.\\d.\\d.+") && SPARK_REPO_URL.contains("cloudera"))
}


class FlowmanConf(settings:Map[String,String]) {
    import FlowmanConf._

    settings.foreach{ case (key,value) => validateSetting(key, value) }

    private def validateSetting(key: String, value: String): Unit = {
        require(key != null, "key cannot be null")
        require(value != null, s"value cannot be null for key: $key")
        val entry = configEntries.get(key)
        if (entry != null) {
            // Only verify configs in the SQLConf object
            entry.valueConverter(value)
        }
    }

    def sparkEnableHive: Boolean = getConf(SPARK_ENABLE_HIVE)
    def hiveAnalyzeTable: Boolean = getConf(HIVE_ANALYZE_TABLE)
    def homeDirectory: Option[File] = getConf(HOME_DIRECTORY)
    def confDirectory: Option[File] = getConf(CONF_DIRECTORY)
    def pluginDirectory: Option[File] = getConf(PLUGIN_DIRECTORY)


    /** Return the value of Spark SQL configuration property for the given key. */
    @throws[NoSuchElementException]("if key is not set")
    def get(key: String): String = {
        settings.get(key).
            orElse {
                // Try to use the default value
                Option(configEntries.get(key)).map { e => e.defaultValueString }
            }.
            getOrElse(throw new NoSuchElementException(key))
    }

    /**
     * Return the `string` value of  configuration property for the given key. If the key is
     * not set yet, return `defaultValue`.
     */
    def get(key: String, defaultValue: String): String = {
        if (defaultValue != null && defaultValue != ConfigEntry.UNDEFINED) {
            val entry = configEntries.get(key)
            if (entry != null) {
                // Only verify configs in the SQLConf object
                entry.valueConverter(defaultValue)
            }
        }
        settings.getOrElse(key, defaultValue)
    }

    /** Get all parameters as a list of pairs */
    def getAll: Array[(String, String)] = {
        settings.toArray
    }

    /**
     * Return the value of configuration property for the given key. If the key is not set
     * yet, return `defaultValue`. This is useful when `defaultValue` in ConfigEntry is not the
     * desired one.
     */
    def getConf[T](entry: ConfigEntry[T], defaultValue: T): T = {
        require(configEntries.get(entry.key) == entry, s"$entry is not registered")
        settings.get(entry.key).map(entry.valueConverter).getOrElse(defaultValue)
    }

    /**
     * Return the value of configuration property for the given key. If the key is not set
     * yet, return `defaultValue` in [[ConfigEntry]].
     */
    def getConf[T](entry: ConfigEntry[T]): T = {
        require(configEntries.get(entry.key) == entry, s"$entry is not registered")
        entry.evaluate(key => settings.get(key))
    }

    /**
     * Return the value of an optional configuration property for the given key. If the key
     * is not set yet, returns None.
     */
    def getConf[T](entry: OptionalConfigEntry[T]): Option[T] = {
        require(configEntries.get(entry.key) == entry, s"$entry is not registered")
        entry.evaluate(key => settings.get(key))
    }

    /**
     * Return whether a given key is set in this [[FlowmanConf]].
     */
    def contains(key: String): Boolean = {
        settings.contains(key)
    }
}
