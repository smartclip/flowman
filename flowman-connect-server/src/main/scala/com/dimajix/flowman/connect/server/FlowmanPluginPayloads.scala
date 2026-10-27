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

package com.dimajix.flowman.connect.server

import org.sparkproject.connect.protobuf.{Any => ProtoAny}

import com.dimajix.flowman.connect.proto.FlowmanConnectCodec
import com.dimajix.flowman.connect.proto.FlowmanMessage


/**
  * Helpers to decode the extension payloads handed to the Spark Connect plugins. Spark Connect passes the plugin the
  * serialized `google.protobuf.Any` (guarded extension), so the plugins undo that wrapping to obtain the boxed
  * [[FlowmanMessage]] via the shared protocol codec.
  */
private[server] object FlowmanPluginPayloads {
    /** Parses the raw extension payload (a serialized `google.protobuf.Any`) into the boxed Flowman protocol message. */
    def decode(bytes:Array[Byte]) : FlowmanMessage = {
        val any = ProtoAny.parseFrom(bytes)
        FlowmanConnectCodec.decode(any.getTypeUrl, any.getValue.toByteArray)
    }
}
