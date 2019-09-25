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

package com.dimajix.flowman.spec.target

import java.io.IOException

import org.apache.hadoop.fs.Path
import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.execution.VerificationFailedException
import com.dimajix.flowman.spec.ObjectMapper
import com.dimajix.flowman.spec.dataset.Dataset
import com.dimajix.flowman.spec.dataset.FileDataset
import com.dimajix.spark.testing.LocalSparkSession


class CompareTargetTest extends FlatSpec with Matchers with LocalSparkSession {
    "The CompareFileTask" should "be parseable from YAML" in {
        val spec =
            """
              |kind: compareFiles
              |expected: test/data/data_1.csv
              |actual: test/data/data_1.csv
              |""".stripMargin
        val target = ObjectMapper.parse[TargetSpec](spec)
        target shouldBe a[CompareTargetSpec]
    }

    it should "work on same files" in {
        val session = Session.builder().build()
        val executor = session.executor
        val context = session.context

        val target = CompareTarget(
            Target.Properties(context),
            FileDataset(Dataset.Properties(context), new Path("test/data/data_1.csv"), "csv"),
            FileDataset(Dataset.Properties(context), new Path("test/data/data_2.csv"), "csv")
        )
        target.expected should be (new Path("test/data/data_1.csv"))
        target.actual should be (new Path("test/data/data_1.csv"))
        noException shouldBe thrownBy(target.execute(executor, Phase.VERIFY))
    }

    it should "fail on non existing actual file" in {
        val session = Session.builder().build()
        val executor = session.executor
        val context = session.context

        val target = CompareTarget(
            Target.Properties(context),
            FileDataset(Dataset.Properties(context), new Path("no_such_file"), "csv"),
            FileDataset(Dataset.Properties(context), new Path("test/data/data_2.csv"), "csv")
        )
        target.expected should be (new Path("test/data/data_1.csv"))
        target.actual should be (new Path("no_such_file"))
        a[VerificationFailedException] shouldBe thrownBy(target.execute(executor, Phase.VERIFY))
    }

    it should "throw an exception on an non existing expected file" in {
        val session = Session.builder().build()
        val executor = session.executor
        val context = session.context

        val target = CompareTarget(
            Target.Properties(context),
            FileDataset(Dataset.Properties(context), new Path("test/data/data_1.csv"), "csv"),
            FileDataset(Dataset.Properties(context), new Path("no_such_file"), "csv")
        )

        target.expected should be (new Path("no_such_file"))
        target.actual should be (new Path("test/data/data_1.csv"))
        an[IOException] shouldBe thrownBy(target.execute(executor, Phase.VERIFY))
    }

    it should "work with a directory as expected" in {
        val session = Session.builder().build()
        val executor = session.executor
        val context = session.context

        val target = CompareTarget(
            Target.Properties(context),
            FileDataset(Dataset.Properties(context), new Path("test/data/data_1.csv"), "csv"),
            FileDataset(Dataset.Properties(context), new Path("test/data"), "csv")
        )

        target.expected should be (new Path("test/data"))
        target.actual should be (new Path("test/data/data_1.csv"))
        noException shouldBe thrownBy(target.execute(executor, Phase.VERIFY))
    }

    it should "work with a directory as actual" in {
        val session = Session.builder().build()
        val executor = session.executor
        val context = session.context

        val target = CompareTarget(
            Target.Properties(context),
            FileDataset(Dataset.Properties(context), new Path("test/data"), "csv"),
            FileDataset(Dataset.Properties(context), new Path("test/data/data_1.csv"), "csv")
        )

        target.expected should be (new Path("test/data/data_1.csv"))
        target.actual should be (new Path("test/data"))
        noException shouldBe thrownBy(target.execute(executor, Phase.VERIFY))
    }

    it should "work with a directory as expected and actual" in {
        val session = Session.builder().build()
        val executor = session.executor
        val context = session.context

        val target = CompareTarget(
            Target.Properties(context),
            FileDataset(Dataset.Properties(context), new Path("test/data/actual"), "csv"),
            FileDataset(Dataset.Properties(context), new Path("test/data/expected"), "csv")
        )

        target.expected should be (new Path("test/data/expected"))
        target.actual should be (new Path("test/data/actual"))
        noException shouldBe thrownBy(target.execute(executor, Phase.VERIFY))
    }
}