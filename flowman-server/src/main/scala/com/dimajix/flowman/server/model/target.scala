/*
 * Copyright 2019-2021 Kaya Kupferschmidt
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

package com.dimajix.flowman.server.model

import java.time.Duration
import java.time.ZonedDateTime


final case class TargetState(
    id:String,
    jobId:Option[String],
    namespace:String,
    project:String,
    version:String,
    target:String,
    partitions:Map[String,String],
    phase:String,
    status:String,
    startDateTime:Option[ZonedDateTime] = None,
    endDateTime:Option[ZonedDateTime] = None,
    duration:Option[Duration] = None,
    error:Option[String] = None
)


final case class TargetStateList(
    data:Seq[TargetState],
    total:Int
)


final case class TargetStateCounts(
    data:Map[String,Int]
)
