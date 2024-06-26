/*
 * Copyright 2021-2022 Kaya Kupferschmidt
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

package com.dimajix.spark.sql.expressions

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.Unevaluable
import org.apache.spark.sql.types.DataType


final case class UnresolvableExpression(expr:String) extends Unevaluable {
    def name: String = expr

    override def dataType: DataType = ???
    override def nullable: Boolean = ???
    override lazy val resolved = false

    override def toString: String = s"'$name"

    override def sql: String = expr

    override def children: Seq[Expression] = Seq()

    /*override*/ protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = this
}
