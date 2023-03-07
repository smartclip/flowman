/*
 * Copyright 2018-2023 Kaya Kupferschmidt
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

package com.dimajix.flowman.grpc;

import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.inprocess.InProcessServerBuilder;


public class InProcessGrpcServer extends GrpcServer {
    final String serverName;

    public InProcessGrpcServer(String serverName, Iterable<GrpcService> services) {
        super(createServer(serverName, services));
        this.serverName = serverName;
    }

    private static Server createServer(String serverName, Iterable<GrpcService> services) {
        InProcessServerBuilder builder = InProcessServerBuilder
            .forName(serverName)
            .directExecutor();

        for (BindableService service : services)
            builder.addService(service);

        return builder.build();
    }
}
