/*
 * Copyright (C) 2026 The Flowman Authors
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

package com.dimajix.flowman.connect.proto

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


class FlowmanConnectMessagesTest extends AnyFlatSpec with Matchers {
    "The FlowmanConnectCodec" should "round-trip FlowmanCapabilities" in {
        val msg = FlowmanCapabilities("1.4.0-SNAPSHOT", "1", Some("parquet"))
        val decoded = FlowmanConnectCodec.decode(msg.typeUrl, msg.toBytes)
        decoded should be (msg)
    }

    it should "round-trip FlowmanCapabilities without optional format" in {
        val msg = FlowmanCapabilities("1.4.0-SNAPSHOT", "1", None)
        val decoded = FlowmanConnectCodec.decode(msg.typeUrl, msg.toBytes)
        decoded should be (msg)
    }

    it should "round-trip FlowmanCountRecords" in {
        val msg = FlowmanCountRecords(Array[Byte](1,2,3,4))
        val decoded = FlowmanConnectCodec.decode(msg.typeUrl, msg.toBytes)
        decoded should be (msg)
        decoded.asInstanceOf[FlowmanCountRecords].input should be (Array[Byte](1,2,3,4))
    }

    it should "round-trip FlowmanDescribe" in {
        val msg = FlowmanDescribe(Array[Byte](9,8,7))
        val decoded = FlowmanConnectCodec.decode(msg.typeUrl, msg.toBytes)
        decoded should be (msg)
    }

    it should "round-trip FlowmanNullableStruct" in {
        val msg = FlowmanNullableStruct(Seq("a", "b"), Seq(Array[Byte](1), Array[Byte](2,2)))
        val decoded = FlowmanConnectCodec.decode(msg.typeUrl, msg.toBytes)
        decoded should be (msg)
        decoded.asInstanceOf[FlowmanNullableStruct].children should be (Seq(Array[Byte](1), Array[Byte](2,2)))
    }

    it should "round-trip FlowmanNullableStruct with unicode strings" in {
        val msg = FlowmanNullableStruct(Seq("straße", "日本語"), Seq(Array[Byte]()))
        val decoded = FlowmanConnectCodec.decode(msg.typeUrl, msg.toBytes)
        decoded should be (msg)
    }

    it should "round-trip FlowmanPing" in {
        val msg = FlowmanPing("1.4.0-SNAPSHOT", "1")
        val decoded = FlowmanConnectCodec.decode(msg.typeUrl, msg.toBytes)
        decoded should be (msg)
    }

    it should "round-trip FlowmanFsCommand" in {
        val msg = FlowmanFsCommand("mkdirs", "/tmp/foo")
        val decoded = FlowmanConnectCodec.decode(msg.typeUrl, msg.toBytes)
        decoded should be (msg)
    }

    it should "tolerate trailing unknown fields" in {
        val msg = FlowmanPing("1.4.0-SNAPSHOT", "1")
        val writer = new java.io.ByteArrayOutputStream()
        writer.write(msg.toBytes)
        // Append an unknown field with a very large field number and a length-delimited payload
        writer.write(Array[Byte](((99.toByte << 3) | 2).toByte))
        writer.write(3)
        writer.write(Array[Byte](1,2,3))
        val decoded = FlowmanConnectCodec.decode(msg.typeUrl, writer.toByteArray)
        decoded should be (msg)
    }

    it should "reject unknown message types" in {
        an [IllegalArgumentException] should be thrownBy
            FlowmanConnectCodec.decode("dimajix.flowman.connect.v1.UnknownMessage", Array.emptyByteArray)
    }
}
