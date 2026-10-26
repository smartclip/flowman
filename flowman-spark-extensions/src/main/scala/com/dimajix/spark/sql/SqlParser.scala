/*
 * Copyright (C) 2019 The Flowman Authors
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

import java.util.Locale

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
                _.collect { case With(_, cteRelations, allowRecursion) =>
                    cteRelations.map(kv => kv._1)
                }
                .flatten
            )
            .toSet

        val cteDependencies =
            allQueries.flatMap (
                _.collect { case With(_,cteRelations, allowRecursion) =>
                    cteRelations
                        .map(kv => kv._2.child)
                        .flatMap(resolveDependencies)
                        .filter(!cteNames.contains(_))
                }
                .flatten
            )
            .toSet

        // Use this regex to unquote identifiers
        val regex = "`(.|$)".r
        val tables =
            allQueries.flatMap (
                _.collect { case p:UnresolvedRelation if !cteNames.contains(p.tableName) => p.tableName }
            )
            .map(regex.replaceAllIn(_,"$1"))
            .toSet

        tables ++ cteDependencies
    }

    /**
      * Returns the index of the first top level `UNION` keyword in the given SQL statement. A top level keyword is
      * one which is neither nested inside parentheses nor inside a string literal or quoted identifier. This provides
      * a client side (and therefore Spark Connect compatible) alternative to parsing the SQL statement into a
      * [[LogicalPlan]] and looking for a [[org.apache.spark.sql.catalyst.plans.logical.Union]] node. Returns `-1` if no
      * such keyword is present.
      */
    def findTopLevelUnion(sql:String) : Int = {
        var depth = 0
        var inSingleQuote = false
        var inDoubleQuote = false
        var inBacktick = false
        var i = 0
        while (i < sql.length) {
            val c = sql.charAt(i)
            if (inSingleQuote) {
                if (c == '\'') {
                    if (i + 1 < sql.length && sql.charAt(i+1) == '\'')
                        i += 1
                    else
                        inSingleQuote = false
                }
            }
            else if (inDoubleQuote) {
                if (c == '"') {
                    if (i + 1 < sql.length && sql.charAt(i+1) == '"')
                        i += 1
                    else
                        inDoubleQuote = false
                }
            }
            else if (inBacktick) {
                if (c == '`')
                    inBacktick = false
            }
            else {
                c match {
                    case '\'' => inSingleQuote = true
                    case '"' => inDoubleQuote = true
                    case '`' => inBacktick = true
                    case '(' => depth += 1
                    case ')' => depth -= 1
                    case u if depth == 0 && (u == 'u' || u == 'U') =>
                        // Detect a UNION keyword (possibly followed by ALL or DISTINCT)
                        val end = Math.min(sql.length, i + 5)
                        if (sql.substring(i, end).toUpperCase(Locale.ROOT) == "UNION") {
                            val prev = if (i > 0) sql.charAt(i-1) else ' '
                            val after = if (end < sql.length) sql.charAt(end) else ' '
                            if (!prev.isLetterOrDigit && !after.isLetterOrDigit)
                                return i
                        }
                        i += 4
                    case _ =>
                }
            }
            i += 1
        }
        -1
    }
}
