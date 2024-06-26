/*
 * Copyright 2018-2022 Kaya Kupferschmidt
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

package com.dimajix.flowman.spec.history

import java.nio.file.Files
import java.nio.file.Path

import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.execution.NoSuchConnectionException
import com.dimajix.flowman.execution.RootContext
import com.dimajix.flowman.spec.ObjectMapper


class JdbcStateStoreTest extends AnyFlatSpec with Matchers with BeforeAndAfter {
    var tempDir:Path = _

    before {
        tempDir = Files.createTempDirectory("jdbc_logged_runner_test")
    }
    after {
        tempDir.toFile.listFiles().foreach(_.delete())
        tempDir.toFile.delete()
    }

    "The JdbcStateStoreSpec" should "throw an exception on missing connection" in {
        val spec =
            """
              |kind: jdbc
              |connection: logger
            """.stripMargin
        val monitor = ObjectMapper.parse[HistorySpec](spec)

        val context = RootContext.builder().build()
        a[NoSuchConnectionException] shouldBe thrownBy(monitor.instantiate(context))
    }

    it should "be parseable" in {
        val spec =
            """
              |kind: jdbc
              |connection: logger
            """.stripMargin

        val monitor = ObjectMapper.parse[HistorySpec](spec)
        monitor shouldBe a[JdbcHistorySpec]
    }

    it should "be parseable with embedded connection" in {
        val spec =
            """
              |kind: jdbc
              |connection:
              |  kind: jdbc
              |  url: some_url
            """.stripMargin

        val monitor = ObjectMapper.parse[HistorySpec](spec)
        monitor shouldBe a[JdbcHistorySpec]
    }
}
