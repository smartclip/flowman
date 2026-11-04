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

import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets


/**
  * The protocol messages exchanged between a Flowman Spark Connect client and the Flowman server plugin.
  *
  * They are transmitted via the "extension" fields of the Spark Connect protocol messages (relations, expressions
  * and commands). The extension payload is a google.protobuf.Any message with `type_url` naming one of the
  * messages below and `value` containing the message encoded in the protobuf wire format.
  *
  * The wire format is implemented manually (see [[FlowmanConnectCodec]]), because generating Java sources from
  * a .proto file via protoc is not possible in an offline build. All fields only use the length-delimited wire
  * type, which keeps the codec small and stable across protobuf versions.
  */
object FlowmanMessages {
    /** The protocol version understood by this version of the shared protocol. Clients and servers must agree on
      * this version for the plugin features to be used. */
    val API_VERSION = "1"

    /** The Flowman version as seen by the server side. Filled in at build time from the Maven project version. */
    val FLOWMAN_VERSION : String = {
        val props = new java.util.Properties()
        val stream = Option(
            classOf[FlowmanMessage].getClassLoader.getResourceAsStream("com/dimajix/flowman/connect/flowman-connect.properties")
        )
        stream.foreach { in =>
            try {
                props.load(in)
            }
            finally {
                in.close()
            }
        }
        Option(props.getProperty("version")).filter(_.nonEmpty).getOrElse("unknown")
    }

    private[proto] val TYPE_URL_PREFIX = "dimajix.flowman.connect.v1"

    private[proto] def typeUrl(name:String) : String = s"$TYPE_URL_PREFIX.$name"
    private[proto] def typeName(typeUrl:String) : String = typeUrl.substring(typeUrl.lastIndexOf('.') + 1)
    def isFlowmanTypeUrl(typeUrl:String) : Boolean = typeUrl.startsWith(TYPE_URL_PREFIX + ".")
}


sealed trait FlowmanMessage {
    def typeUrl : String
    def toBytes : Array[Byte]
}


/**
  * Relation extension. Returns a single row with the version and capability information of the server plugin.
  * Also acts as a liveness/handshake probe: executing this relation only succeeds when the Flowman relation
  * plugin is installed on the server.
  *
  * @param flowmanVersion  The Flowman version running inside the server
  * @param apiVersion      The protocol version supported by the server plugin
  * @param format          An optional format name to probe for multi-path support
  */
final case class FlowmanCapabilities(
    flowmanVersion:String,
    apiVersion:String,
    format:Option[String]
) extends FlowmanMessage {
    override def typeUrl : String = FlowmanMessages.typeUrl("FlowmanCapabilities")
    override def toBytes : Array[Byte] = FlowmanConnectCodec.encodeCapabilities(this)
}


/**
  * Relation extension. Wraps an input relation in a server-side `CountRecords` node so that the number of rows
  * passing through the input is counted during query execution (as opposed to the extra `df.count()` action which
  * has to be used without the plugin).
  *
  * @param input  The serialized Spark Connect input relation (message bytes of the root relation)
  */
final case class FlowmanCountRecords(input:Array[Byte]) extends FlowmanMessage {
    override def typeUrl : String = FlowmanMessages.typeUrl("FlowmanCountRecords")
    override def toBytes : Array[Byte] = FlowmanConnectCodec.encodeCountRecords(this)
}


/**
  * Relation extension. Enriches the schema of an input relation with the "described" schema as computed by the
  * classic backend: CHAR/VARCHAR lengths and column descriptions are recovered from the analyzed logical plan on
  * the server. Returns a single row with a single column containing the enriched Spark schema as JSON.
  *
  * @param input  The serialized Spark Connect input relation (message bytes of the root relation)
  */
final case class FlowmanDescribe(input:Array[Byte]) extends FlowmanMessage {
    override def typeUrl : String = FlowmanMessages.typeUrl("FlowmanDescribe")
    override def toBytes : Array[Byte] = FlowmanConnectCodec.encodeDescribe(this)
}


/**
  * Expression extension. Creates a struct which evaluates to NULL if and only if all children evaluate to NULL.
  * This mirrors the classic `named_nullable_struct` expression, including the propagation of column metadata from
  * named children into the struct field metadata, which is not possible via the public (connect) API.
  *
  * @param names     Field names of the struct, one per child
  * @param children  The serialized Spark Connect child expressions (message bytes, one per child)
  */
final case class FlowmanNullableStruct(names:Seq[String], children:Seq[Array[Byte]]) extends FlowmanMessage {
    override def typeUrl : String = FlowmanMessages.typeUrl("FlowmanNullableStruct")
    override def toBytes : Array[Byte] = FlowmanConnectCodec.encodeNullableStruct(this)
}


/**
  * Command extension. Used by the client to detect the presence of the Flowman server plugin and to verify that
  * the protocol versions match ("version handshake").
  *
  * @param flowmanVersion  The Flowman version of the client
  * @param apiVersion      The protocol version the client understands
  */
final case class FlowmanPing(flowmanVersion:String, apiVersion:String) extends FlowmanMessage {
    override def typeUrl : String = FlowmanMessages.typeUrl("FlowmanPing")
    override def toBytes : Array[Byte] = FlowmanConnectCodec.encodePing(this)
}


/**
  * Command extension. Executes a file system operation on the server side. This is used for server-side FS checks
  * on platforms where the client has no direct access to the cluster storage.
  *
  * @param operation  One of "mkdirs", "delete"
  * @param path       The path to operate on
  */
final case class FlowmanFsCommand(operation:String, path:String) extends FlowmanMessage {
    override def typeUrl : String = FlowmanMessages.typeUrl("FlowmanFsCommand")
    override def toBytes : Array[Byte] = FlowmanConnectCodec.encodeFsCommand(this)
}


/**
  * Minimal protobuf wire format codec for the [[FlowmanMessage]]s. Only the length-delimited wire type (2) is
  * used, which keeps this implementation tiny and independent of a specific protobuf runtime version.
  */
object FlowmanConnectCodec {
    /** Wire type for length-delimited fields (strings and byte arrays) */
    private final val WIRE_LENGTH_DELIMITED = 2
    private final val WIRE_64BIT = 1
    private final val WIRE_VARINT = 0
    private final val WIRE_32BIT = 5

    private final class Writer {
        private val buffer = new ByteArrayOutputStream()
        def writeVarint(value:Long) : Unit = {
            var v = value
            while ((v & ~0x7FL) != 0L) {
                buffer.write(((v & 0x7F) | 0x80).toInt)
                v = v >>> 7
            }
            buffer.write(v.toInt)
        }
        def writeTag(field:Int, wire:Int) : Unit = writeVarint((field.toLong << 3) | wire)
        def writeString(field:Int, value:String) : Unit = {
            val bytes = value.getBytes(StandardCharsets.UTF_8)
            writeTag(field, WIRE_LENGTH_DELIMITED)
            writeVarint(bytes.length)
            buffer.write(bytes)
        }
        def writeBytes(field:Int, value:Array[Byte]) : Unit = {
            writeTag(field, WIRE_LENGTH_DELIMITED)
            writeVarint(value.length)
            buffer.write(value)
        }
        def toByteArray : Array[Byte] = buffer.toByteArray
    }

    private final class Reader(data:Array[Byte]) {
        private var pos = 0

        def hasRemaining : Boolean = pos < data.length

        private def require(count:Long) : Unit = {
            if (count < 0 || count > data.length - pos)
                throw new IllegalArgumentException(
                    s"Malformed Flowman message: field length $count exceeds remaining payload ${data.length - pos}"
                )
        }

        def readVarint() : Long = {
            var shift = 0
            var result = 0L
            while (shift < 64) {
                if (pos >= data.length)
                    throw new IllegalArgumentException("Malformed Flowman message: truncated varint")
                val b = data(pos); pos += 1
                result |= (b & 0x7FL).toLong << shift
                if ((b & 0x80) == 0)
                    return result
                shift += 7
            }
            throw new IllegalArgumentException("Malformed Flowman message: varint too long")
        }
        private def readTag() : (Int,Int) = {
            val tag = readVarint()
            val field = (tag >>> 3).toInt
            if (field <= 0)
                throw new IllegalArgumentException(s"Malformed Flowman message: invalid field number $field")
            field -> (tag & 0x7).toInt
        }
        private def readLength() : Int = {
            val length = readVarint()
            require(length)
            length.toInt
        }
        def readString() : String = {
            val length = readLength()
            val str = new String(data, pos, length, StandardCharsets.UTF_8)
            pos += length
            str
        }
        def readBytes() : Array[Byte] = {
            val length = readLength()
            val bytes = java.util.Arrays.copyOfRange(data, pos, pos + length)
            pos += length
            bytes
        }
        def skipField(wire:Int) : Unit = {
            wire match {
                case WIRE_VARINT => readVarint()
                case WIRE_64BIT =>
                    require(8)
                    pos += 8
                case WIRE_LENGTH_DELIMITED =>
                    val length = readLength()
                    pos += length
                case WIRE_32BIT =>
                    require(4)
                    pos += 4
                case other => throw new IllegalArgumentException(s"Unknown wire type $other")
            }
        }
        def readTagAndSkipUnknown(known:Set[Int]) : Option[(Int,Int)] = {
            var result : Option[(Int,Int)] = None
            var done = false
            while (!done && hasRemaining) {
                val (field, wire) = readTag()
                if (known.contains(field)) {
                    result = Some(field -> wire)
                    done = true
                }
                else {
                    skipField(wire)
                }
            }
            result
        }
        def ensureFinished(field:Int) : Unit = {
            while (hasRemaining) {
                val (_, wire) = readTag()
                skipField(wire)
            }
        }
    }

    def encodeCapabilities(msg:FlowmanCapabilities) : Array[Byte] = {
        val writer = new Writer()
        writer.writeString(1, msg.flowmanVersion)
        writer.writeString(2, msg.apiVersion)
        msg.format.foreach(f => writer.writeString(3, f))
        writer.toByteArray
    }
    private def decodeCapabilities(reader:Reader) : FlowmanCapabilities = {
        var flowmanVersion = ""
        var apiVersion = ""
        var format:Option[String] = None
        var done = false
        while (!done) {
            reader.readTagAndSkipUnknown(Set(1,2,3)) match {
                case Some((1,2)) => flowmanVersion = reader.readString()
                case Some((2,2)) => apiVersion = reader.readString()
                case Some((3,2)) => format = Some(reader.readString())
                case None => done = true
                case _ => throw new IllegalArgumentException("Malformed FlowmanCapabilities")
            }
        }
        FlowmanCapabilities(flowmanVersion, apiVersion, format)
    }

    def encodeCountRecords(msg:FlowmanCountRecords) : Array[Byte] = {
        val writer = new Writer()
        writer.writeBytes(1, msg.input)
        writer.toByteArray
    }
    private def decodeCountRecords(reader:Reader) : FlowmanCountRecords = {
        var input:Array[Byte] = Array.empty
        var done = false
        while (!done) {
            reader.readTagAndSkipUnknown(Set(1)) match {
                case Some((1,2)) => input = reader.readBytes()
                case None => done = true
                case _ => throw new IllegalArgumentException("Malformed FlowmanCountRecords")
            }
        }
        FlowmanCountRecords(input)
    }

    def encodeDescribe(msg:FlowmanDescribe) : Array[Byte] = {
        val writer = new Writer()
        writer.writeBytes(1, msg.input)
        writer.toByteArray
    }
    private def decodeDescribe(reader:Reader) : FlowmanDescribe = {
        var input:Array[Byte] = Array.empty
        var done = false
        while (!done) {
            reader.readTagAndSkipUnknown(Set(1)) match {
                case Some((1,2)) => input = reader.readBytes()
                case None => done = true
                case _ => throw new IllegalArgumentException("Malformed FlowmanDescribe")
            }
        }
        FlowmanDescribe(input)
    }

    def encodeNullableStruct(msg:FlowmanNullableStruct) : Array[Byte] = {
        val writer = new Writer()
        msg.names.foreach(n => writer.writeString(1, n))
        msg.children.foreach(c => writer.writeBytes(2, c))
        writer.toByteArray
    }
    private def decodeNullableStruct(reader:Reader) : FlowmanNullableStruct = {
        val names = scala.collection.mutable.ArrayBuffer[String]()
        val children = scala.collection.mutable.ArrayBuffer[Array[Byte]]()
        var done = false
        while (!done) {
            reader.readTagAndSkipUnknown(Set(1,2)) match {
                case Some((1,2)) => names += reader.readString()
                case Some((2,2)) => children += reader.readBytes()
                case None => done = true
                case _ => throw new IllegalArgumentException("Malformed FlowmanNullableStruct")
            }
        }
        FlowmanNullableStruct(names.toSeq, children.toSeq)
    }

    def encodePing(msg:FlowmanPing) : Array[Byte] = {
        val writer = new Writer()
        writer.writeString(1, msg.flowmanVersion)
        writer.writeString(2, msg.apiVersion)
        writer.toByteArray
    }
    private def decodePing(reader:Reader) : FlowmanPing = {
        var flowmanVersion = ""
        var apiVersion = ""
        var done = false
        while (!done) {
            reader.readTagAndSkipUnknown(Set(1,2)) match {
                case Some((1,2)) => flowmanVersion = reader.readString()
                case Some((2,2)) => apiVersion = reader.readString()
                case None => done = true
                case _ => throw new IllegalArgumentException("Malformed FlowmanPing")
            }
        }
        FlowmanPing(flowmanVersion, apiVersion)
    }

    def encodeFsCommand(msg:FlowmanFsCommand) : Array[Byte] = {
        val writer = new Writer()
        writer.writeString(1, msg.operation)
        writer.writeString(2, msg.path)
        writer.toByteArray
    }
    private def decodeFsCommand(reader:Reader) : FlowmanFsCommand = {
        var operation = ""
        var path = ""
        var done = false
        while (!done) {
            reader.readTagAndSkipUnknown(Set(1,2)) match {
                case Some((1,2)) => operation = reader.readString()
                case Some((2,2)) => path = reader.readString()
                case None => done = true
                case _ => throw new IllegalArgumentException("Malformed FlowmanFsCommand")
            }
        }
        FlowmanFsCommand(operation, path)
    }

    /**
      * Decodes a message from the payload stored in the `value` field of a google.protobuf.Any extension.
      * @param typeUrl  The `type_url` of the Any message
      * @param payload  The `value` bytes of the Any message
      */
    def decode(typeUrl:String, payload:Array[Byte]) : FlowmanMessage = {
        val reader = new Reader(payload)
        FlowmanMessages.typeName(typeUrl) match {
            case "FlowmanCapabilities" => decodeCapabilities(reader)
            case "FlowmanCountRecords" => decodeCountRecords(reader)
            case "FlowmanDescribe" => decodeDescribe(reader)
            case "FlowmanNullableStruct" => decodeNullableStruct(reader)
            case "FlowmanPing" => decodePing(reader)
            case "FlowmanFsCommand" => decodeFsCommand(reader)
            case other => throw new IllegalArgumentException(s"Unknown Flowman connect message: $other")
        }
    }

    def encode(msg:FlowmanMessage) : Array[Byte] = msg.toBytes
}
