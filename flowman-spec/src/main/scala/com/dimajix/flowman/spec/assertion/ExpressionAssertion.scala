/*
 * Copyright 2021 Kaya Kupferschmidt
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

package com.dimajix.flowman.spec.assertion

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.expr
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.model.Assertion
import com.dimajix.flowman.model.AssertionResult
import com.dimajix.flowman.model.AssertionTestResult
import com.dimajix.flowman.model.BaseAssertion
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.model.ResourceIdentifier
import com.dimajix.spark.sql.DataFrameUtils


case class ExpressionAssertion(
    override val instanceProperties:Assertion.Properties,
    mapping:MappingOutputIdentifier,
    expected:Seq[String]
) extends BaseAssertion {
    private val logger = LoggerFactory.getLogger(classOf[SqlAssertion])

    /**
      * Returns a list of physical resources required by this assertion. This list will only be non-empty for assertions
      * which actually read from physical data.
      *
      * @return
      */
    override def requires: Set[ResourceIdentifier] = Set()

    /**
      * Returns the dependencies (i.e. names of tables in the Dataflow model)
      *
      * @return
      */
    override def inputs: Seq[MappingOutputIdentifier] = Seq(mapping)

    /**
      * Executes this [[Assertion]] and returns a corresponding DataFrame
      *
      * @param execution
      * @param input
      * @return
      */
    override def execute(execution: Execution, input: Map[MappingOutputIdentifier, DataFrame]): AssertionResult = {
        require(execution != null)
        require(input != null)

        AssertionResult.of(this) {
            val mapping = input(this.mapping)

            expected.map { test =>
                AssertionTestResult.of(test) {
                    DataFrameUtils.withCache(mapping.filter(!expr(test)).limit(21)) { df =>
                        if (df.count() > 0) {
                            val columns = CatalystSqlParser.parseExpression(test).references.map(_.name).toSeq.distinct.map(col)
                            val diff = DataFrameUtils.showString(mapping.select(columns: _*), 20, -1)
                            logger.error(s"failed expectation: $test\n$diff")
                            false
                        }
                        else {
                            true
                        }
                    }
                }
            }
        }
    }
}


class ExpressionAssertionSpec extends AssertionSpec {
    @JsonProperty(value="mapping", required=true) private var mapping:String = ""
    @JsonProperty(value="expected", required=false) private var expected:Seq[String] = Seq()

    override def instantiate(context: Context, properties:Option[Assertion.Properties] = None): ExpressionAssertion = {
        ExpressionAssertion(
            instanceProperties(context, properties),
            MappingOutputIdentifier(context.evaluate(mapping)),
            expected.map(context.evaluate)
        )
    }
}
