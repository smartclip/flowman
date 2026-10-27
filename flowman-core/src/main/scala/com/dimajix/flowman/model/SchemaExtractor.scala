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

package com.dimajix.flowman.model

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.expressions.CaseWhen
import org.apache.spark.sql.catalyst.expressions.Cast
import org.apache.spark.sql.catalyst.expressions.Coalesce
import org.apache.spark.sql.catalyst.expressions.Concat
import org.apache.spark.sql.catalyst.expressions.DateFormatClass
import org.apache.spark.sql.catalyst.expressions.ExprId
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.FromUTCTimestamp
import org.apache.spark.sql.catalyst.expressions.FromUnixTime
import org.apache.spark.sql.catalyst.expressions.If
import org.apache.spark.sql.catalyst.expressions.IfNull
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.expressions.Nvl
import org.apache.spark.sql.catalyst.expressions.Nvl2
import org.apache.spark.sql.catalyst.expressions.String2StringExpression
import org.apache.spark.sql.catalyst.expressions.Substring
import org.apache.spark.sql.catalyst.expressions.ToUTCTimestamp
import org.apache.spark.sql.catalyst.expressions.ToUnixTimestamp
import org.apache.spark.sql.catalyst.expressions.TruncInstant
import org.apache.spark.sql.catalyst.expressions.UnaryExpression
import org.apache.spark.sql.catalyst.expressions.UnixTimestamp
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.Union
import org.apache.spark.sql.types.StructField
import org.apache.spark.unsafe.types.UTF8String

import com.dimajix.flowman.types.CharType
import com.dimajix.flowman.types.Field
import com.dimajix.flowman.types.FieldType
import com.dimajix.flowman.types.SchemaUtils.coerce
import com.dimajix.flowman.types.StructType
import com.dimajix.flowman.types.VarcharType
import com.dimajix.spark.sql.SchemaUtils.hasExtendedTypeinfo


/**
  * Recovers the "rich" schema (incl. CHAR/VARCHAR lengths and column descriptions) of a DataFrame from its
  * analyzed logical plan. This is used by the (classic) backend for the `describe` operation of mappings and by
  * the Flowman Spark Connect server plugin to provide the same information to a Connect client.
  */
object SchemaExtractor {
    def collectExpressions(plan:LogicalPlan) : Map[ExprId, NamedExpression] = {
        plan match {
            // Special handling for UNIONs, which otherwise would only collect columns of first UNION child
            case union:Union =>
                val expressions = union.output
                val childExpressions = union.children.map(resolveAttributes)
                // The Alias(Coalesce()) expression is a workaround to collect all columns of all children into
                // a single expression, which also needs to be a NamedExpression
                expressions.zipWithIndex.map { case(e,i) => e.exprId -> Alias(Coalesce(childExpressions.map(_(i))),e.name)() }.toMap
            case plan:LogicalPlan =>
                val expressions = plan.expressions.collect { case n:NamedExpression => n.exprId -> n }.toMap
                val childExpressions = plan.children.flatMap(collectExpressions)
                expressions ++ childExpressions
        }
    }

    def resolveAttributes(plan:LogicalPlan) : Seq[NamedExpression] = {
        val expressions = collectExpressions(plan)
        val output = plan.output
        output.map(a => expressions.getOrElse(a.exprId, a))
    }

    private def extractComment2(expressions: Seq[Expression]) : Option[String] = {
        expressions.flatMap(extractComment).headOption
    }
    def extractComment(expression: Expression) : Option[String] = {
        expression match {
            case n:NamedExpression if n.metadata.contains("comment") => Some(n.metadata.getString("comment"))
            case c:Cast => extractComment(c.child)
            case a:Alias => extractComment(a.child)
            case c:Coalesce => extractComment2(c.children)
            case a:AggregateExpression => extractComment(a.aggregateFunction)
            case a:AggregateFunction => extractComment2(a.children).map(_ + s" (${a.prettyName.toUpperCase})")
            case s:String2StringExpression => extractComment(s.asInstanceOf[UnaryExpression].child)
            case i:If => extractComment(i.trueValue).orElse(extractComment(i.falseValue))
            case n:IfNull => extractComment(n.left).orElse(extractComment(n.right))
            case n:Nvl => extractComment(n.left).orElse(extractComment(n.right))
            case n:Nvl2 => extractComment(n.expr2).orElse(extractComment(n.expr3))
            case c:CaseWhen if c.branches.size == 1 => extractComment(c.branches.head._2)
            case t:TruncInstant => extractComment(t.right)
            case f:DateFormatClass => extractComment(f.left)
            case u:UnixTimestamp => extractComment(u.left)
            case u:FromUnixTime => extractComment(u.left)
            case t:ToUnixTimestamp => extractComment(t.left)
            case u:FromUTCTimestamp => extractComment(u.left)
            case u:ToUTCTimestamp => extractComment(u.left)
            case _ => None
        }
    }

    def extractType(expression: Expression) : FieldType = {
        expression match {
            case n:NamedExpression if hasExtendedTypeinfo(n.metadata) => Field.of(StructField(n.name, n.dataType, n.nullable, n.metadata)).ftype
            case a:Alias => extractType(a.child)
            case l:Literal if l.dataType == org.apache.spark.sql.types.StringType =>
                l.value match {
                    case str:UTF8String => CharType(str.numChars())
                    case _ => FieldType.of(l.dataType)
                }
            case c:Coalesce => c.children.map(extractType).reduce(coerce)
            case c:Concat =>
                c.children.map(extractType).reduce { (lt,rt) =>
                    (lt,rt) match {
                        case (VarcharType(l), VarcharType(r)) => VarcharType(l + r)
                        case (CharType(l), VarcharType(r)) => VarcharType(l + r)
                        case (VarcharType(l), CharType(r)) => VarcharType(l + r)
                        case (CharType(l), CharType(r)) => CharType(l + r)
                        case _ => FieldType.of(expression.dataType)
                    }
                }
            case c:Substring =>
                (extractType(c.str), c.pos, c.len) match {
                    case (VarcharType(l), Literal(pos:Int, _), Literal(len:Int, _)) => VarcharType(math.max(math.min(l-pos+1, len), 1))
                    case (CharType(l), Literal(pos:Int, _), Literal(len:Int, _)) => CharType(math.max(math.min(l-pos+1, len), 1))
                    case _ => FieldType.of(expression.dataType)
                }
            case s:String2StringExpression => extractType(s.asInstanceOf[UnaryExpression].child)
            case _ => FieldType.of(expression.dataType)
        }
    }

    def extractSchema(df:DataFrame) : StructType = {
        extractSchema(df.queryExecution.analyzed)
    }

    def extractSchema(plan:LogicalPlan) : StructType = {
        val output = plan.output
        val expressions = collectExpressions(plan)
        val attributes = output.map(a => a.name -> expressions.getOrElse(a.exprId, a))
            .map { case (name,expr) =>
                val field = Field.of(StructField(name, expr.dataType, expr.nullable, expr.metadata))
                field.copy(
                    ftype = extractType(expr),
                    description = extractComment(expr).orElse(field.description)
                )
            }

        StructType(attributes)
    }
}
