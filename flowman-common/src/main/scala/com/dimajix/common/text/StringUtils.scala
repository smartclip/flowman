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

package com.dimajix.common.text

object StringUtils {
    def truncate(str:String, maxLength: Int) : String = {
        require(maxLength > 3)
        if (str == null) {
            null
        }
        else {
            val length = str.length
            if (length > maxLength) str.substring(0, maxLength - 3) + "..." else str.substring(0, length)
        }
    }
}
