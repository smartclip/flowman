/*
 * Copyright 2019 Kaya Kupferschmidt
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

package com.dimajix.spark.sql

import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.With



object SqlParser {
    /**
     * A thin wrapper for parsing a SQL statement into a Spark Catalyst logical plan
     * @param sql
     * @return
     */
    def parsePlan(sql:String) : LogicalPlan = {
        CatalystSqlParser.parsePlan(sql)
    }

    /**
     * Analyzes the SQL statement and returns all required dependencies (i.e. table names)
     * @param sql
     * @return
     */
    def resolveDependencies(sql:String) : Set[String] = {
        val plan = parsePlan(sql)
        resolveDependencies(plan)
    }

    def resolveDependencies(plan:LogicalPlan) : Set[String] = {
        // Do not use LogicalPlan.collectWithSubqueries to provide compatibility with Spark 2.4
        def collectAllSubqueries(plan:LogicalPlan) : Seq[LogicalPlan] = {
            val subqueries = plan.flatMap(_.subqueries)
            subqueries ++ subqueries.flatMap(collectAllSubqueries)
        }
        val allQueries = plan +: collectAllSubqueries(plan)

        val cteNames =
            allQueries.flatMap (
                _.collect { case With(_, cteRelations) =>
                    cteRelations.map(kv => kv._1)
                }
                .flatten
            )
            .toSet

        val cteDependencies =
            allQueries.flatMap (
                _.collect { case With(_,cteRelations) =>
                    cteRelations
                        .map(kv => kv._2.child)
                        .flatMap(resolveDependencies)
                        .filter(!cteNames.contains(_))
                }
                .flatten
            )
            .toSet

        val tables =
            allQueries.flatMap (
                _.collect { case p:UnresolvedRelation if !cteNames.contains(p.tableName) => p.tableName }
            )
            .toSet

        tables ++ cteDependencies
    }
}
