/*
 * Copyright 2019-2022 Kaya Kupferschmidt
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

package com.dimajix.flowman.spec.target

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.spark.sql.Row
import org.slf4j.LoggerFactory

import com.dimajix.common.No
import com.dimajix.common.Trilean
import com.dimajix.common.Yes
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.VerificationFailedException
import com.dimajix.flowman.model.BaseTarget
import com.dimajix.flowman.model.Dataset
import com.dimajix.flowman.model.ResourceIdentifier
import com.dimajix.flowman.model.Target
import com.dimajix.flowman.spec.dataset.DatasetSpec
import com.dimajix.flowman.transforms.SchemaEnforcer
import com.dimajix.spark.sql.DataFrameUtils


case class CompareTarget(
    instanceProperties:Target.Properties,
    actual:Dataset,
    expected:Dataset
) extends BaseTarget {
    private val logger = LoggerFactory.getLogger(classOf[CompareTarget])

    /**
     * Returns all phases which are implemented by this target in the execute method
     * @return
     */
    override def phases : Set[Phase] = Set(Phase.VERIFY)

    /**
     * Returns a list of physical resources required by this target
     *
     * @return
     */
    override def requires(phase: Phase): Set[ResourceIdentifier] = {
        phase match {
            case Phase.VERIFY => actual.requires ++ expected.requires
            case _ => Set()
        }
    }

    /**
     * Returns the state of the target, specifically of any artifacts produces. If this method return [[Yes]],
     * then an [[execute]] should update the output, such that the target is not 'dirty' any more.
     *
     * @param execution
     * @param phase
     * @return
     */
    override def dirty(execution: Execution, phase: Phase): Trilean = {
        phase match {
            case Phase.VERIFY => Yes
            case _ => No
        }
    }

    /**
      * Performs a verification of the build step or possibly other checks.
      *
      * @param executor
      */
    override protected def verify(executor: Execution): Unit = {
        logger.info(s"Comparing actual dataset '${actual.name}' with expected dataset '${expected.name}'")
        val expectedDf = expected.read(executor)
        val actualDf = try {
            actual.read(executor)
        }
        catch {
            case ex:Exception => throw new VerificationFailedException(this.identifier, ex)
        }

        // TODO: Compare schemas
        val xfs = SchemaEnforcer(expectedDf.schema)
        val conformedDf = xfs.transform(actualDf)

        val diff = DataFrameUtils.diff(expectedDf, conformedDf)
        if (diff.nonEmpty) {
            logger.error(s"Dataset '${actual.name}' does not equal the expected dataset '${expected.name}'")
            logger.error(s"Difference between datasets: \n${diff.get}")
            throw new VerificationFailedException(identifier)
        }
        else {
            logger.info(s"Dataset '${actual.name}' matches the expected dataset '${expected.name}'")
        }
    }
}


class CompareTargetSpec extends TargetSpec {
    @JsonProperty(value = "actual", required = true) private var actual: DatasetSpec = _
    @JsonProperty(value = "expected", required = true) private var expected: DatasetSpec = _

    override def instantiate(context: Context, properties:Option[Target.Properties] = None): CompareTarget = {
        CompareTarget(
            instanceProperties(context, properties),
            actual.instantiate(context),
            expected.instantiate(context)
        )
    }
}
