/*
 * Copyright 2018-2019 Kaya Kupferschmidt
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

package com.dimajix.flowman.execution

import java.util.Locale

import scala.collection.mutable
import scala.util.Failure
import scala.util.Success
import scala.util.Try

import com.dimajix.flowman.common.ThreadUtils
import com.dimajix.flowman.model.Result


sealed abstract class Status extends Product with Serializable {
    def lower : String = toString.toLowerCase(Locale.ROOT)
    def upper : String = toString.toUpperCase(Locale.ROOT)

    /**
     * Returns [[true]] if this Status represents an successful operation (i.e. SUCCESS or SKIPPED)
     * @return
     */
    final def success : Boolean = this match {
        case Status.SUCCESS|Status.SUCCESS_WITH_ERRORS|Status.SKIPPED => true
        case _ => false
    }

    /**
     * Returns [[true]] is this Status has to be interpreted as an unsuccessful operation
     * @return
     */
    final def failure : Boolean = !success
}

object Status {
    case object UNKNOWN extends Status
    case object RUNNING extends Status
    case object SUCCESS extends Status
    case object SUCCESS_WITH_ERRORS extends Status
    case object FAILED extends Status
    case object ABORTED extends Status
    case object SKIPPED extends Status

    def ofString(status:String) : Status = {
        status.toLowerCase(Locale.ROOT) match {
            case "unknown" => UNKNOWN
            case "running" => RUNNING
            case "success" => SUCCESS
            case "success_with_errors" => SUCCESS_WITH_ERRORS
            case "failed" => FAILED
            case "aborted"|"killed" => ABORTED
            case "skipped" => SKIPPED
            case _ => throw new IllegalArgumentException(s"No status defined for '$status'")
        }
    }

    /**
      * This function determines a common status of multiple actions
      * @param seq
      * @param fn
      * @tparam T
      * @return
      */
    def ofAll[T](seq: Iterable[T], keepGoing:Boolean=false)(fn:T => Status) : Status = {
        val iter = seq.iterator
        var error = false
        var success_with_errors = false
        var skipped = true
        val empty = !iter.hasNext
        while (iter.hasNext && (!error || keepGoing)) {
            val item = iter.next()
            val status = fn(item)
            error |= status.failure
            success_with_errors |= (status == Status.SUCCESS_WITH_ERRORS)
            skipped &= (status == Status.SKIPPED)
        }

        if (empty)
            Status.SUCCESS
        else if (error)
            Status.FAILED
        else if (skipped)
            Status.SKIPPED
        else if (success_with_errors)
            Status.SUCCESS_WITH_ERRORS
        else
            Status.SUCCESS
    }

    def ofAll(seq: Iterable[Status]) : Status = {
        val iter = seq.iterator
        var error = false
        var success_with_errors = false
        var skipped = true
        val empty = !iter.hasNext
        while (iter.hasNext && (!error)) {
            val status = iter.next()
            error |= status.failure
            success_with_errors |= (status == Status.SUCCESS_WITH_ERRORS)
            skipped &= (status == Status.SKIPPED)
        }

        if (empty)
            Status.SUCCESS
        else if (error)
            Status.FAILED
        else if (skipped)
            Status.SKIPPED
        else if (success_with_errors)
            Status.SUCCESS_WITH_ERRORS
        else
            Status.SUCCESS
    }

    /**
     * This function determines a common status of multiple actions, which are executed in parallel
     * @param seq
     * @param fn
     * @tparam T
     * @return
     */
    def parallelOfAll[T](seq: Seq[T], parallelism:Int, keepGoing:Boolean=false, prefix:String="ParallelExecution")(fn:T => Status) : Status = {
        // Allocate state variables for tracking overall Status
        val statusLock = new Object
        var error = false
        var skipped = true
        var empty = true
        var success_with_errors = false

        ThreadUtils.parmap(seq, prefix, parallelism) { p =>
            if (!error || keepGoing) {
                val result = Try {
                    fn(p)
                }
                // Evaluate status
                statusLock.synchronized {
                    result match {
                        case Success(status) =>
                            empty = false
                            error |= status.failure
                            success_with_errors |= (status == Status.SUCCESS_WITH_ERRORS)
                            skipped &= (status == Status.SKIPPED)
                            status
                        case Failure(_) =>
                            empty = false
                            error = true
                            Status.FAILED
                    }
                }
            }
        }

        // Evaluate overall status
        if (empty)
            Status.SUCCESS
        else if (error)
            Status.FAILED
        else if (skipped)
            Status.SKIPPED
        else if (success_with_errors)
            Status.SUCCESS_WITH_ERRORS
        else
            Status.SUCCESS
    }
}
