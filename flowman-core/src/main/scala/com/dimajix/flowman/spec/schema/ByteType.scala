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

package com.dimajix.flowman.spec.schema

import org.apache.spark.sql.types.DataType


case object ByteType extends FieldType {
    override def sparkType : DataType = org.apache.spark.sql.types.ByteType

    override def parse(value:String, granularity:String) : Byte = {
        if (granularity != null && granularity.nonEmpty)
            (value.toByte / granularity.toByte * granularity.toByte).toByte
        else
            value.toByte
    }
    override def interpolate(value: FieldValue, granularity:String) : Iterable[Byte] = {
        value match {
            case SingleValue(v) => Seq(parse(v, granularity))
            case ArrayValue(values) => values.map(v => parse(v, granularity))
            case RangeValue(start,end,step) => {
                if (step != null && step.nonEmpty) {
                    val range = start.toInt.until(end.toInt).by(step.toInt)
                    if (granularity != null && granularity.nonEmpty) {
                        val mod = granularity.toInt
                        range.map(_ / mod * mod).distinct.map(_.toByte)
                    }
                    else {
                        range.map(_.toByte)
                    }
                }
                else if (granularity != null && granularity.nonEmpty) {
                    val mod = granularity.toInt
                    (start.toInt / mod * mod).until(end.toInt / mod * mod).by(mod).map(_.toByte)
                }
                else {
                    start.toInt.until(end.toInt).map(_.toByte)
                }
            }
        }
    }
}