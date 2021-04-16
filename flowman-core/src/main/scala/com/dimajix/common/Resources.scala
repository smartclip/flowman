/*
 * Copyright 2021 Kaya Kupferschmidt
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

package com.dimajix.common

import java.net.URL
import java.util.Properties


class Resources
object Resources {
    def loadProperties(resourceName:String) : Properties = {
        val url = classOf[Resources].getClassLoader.getResource(resourceName)
        loadProperties(url)
    }

    def loadProperties(contextClass:Class[_], resourceName:String) : Properties = {
        val url = com.google.common.io.Resources.getResource(contextClass, resourceName)
        loadProperties(url)
    }

    private def loadProperties(url:URL) : Properties = {
        val byteSource = com.google.common.io.Resources.asByteSource(url)
        val inputStream = byteSource.openBufferedStream
        try {
            val props = new Properties()
            val properties = props.load(inputStream)
            props.load(inputStream)
            props
        } finally {
            inputStream.close()
        }
    }
}
