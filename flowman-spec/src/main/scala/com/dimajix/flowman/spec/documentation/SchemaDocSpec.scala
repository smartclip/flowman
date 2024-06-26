/*
 * Copyright 2022 Kaya Kupferschmidt
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

package com.dimajix.flowman.spec.documentation

import com.fasterxml.jackson.annotation.JsonProperty

import com.dimajix.flowman.documentation.Reference
import com.dimajix.flowman.documentation.SchemaDoc
import com.dimajix.flowman.execution.Context


class SchemaDocSpec {
    @JsonProperty(value="description", required=false) private var description:Option[String] = None
    @JsonProperty(value="columns", required=false) private var columns:Seq[ColumnDocSpec] = Seq()
    @JsonProperty(value="checks", required=false) private var checks:Seq[SchemaCheckSpec] = Seq()

    def instantiate(context: Context, parent:Reference): SchemaDoc = {
        val doc = SchemaDoc(
            Some(parent),
            description = context.evaluate(description)
        )
        val ref = doc.reference

        val cols = columns.map(_.instantiate(context, ref))
        val tests = this.checks.map(_.instantiate(context, ref))
        doc.copy(
            columns = cols,
            checks = tests
        )
    }
}
