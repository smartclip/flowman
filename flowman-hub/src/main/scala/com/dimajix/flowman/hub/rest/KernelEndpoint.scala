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

package com.dimajix.flowman.hub.rest

import java.time.Clock
import java.time.ZonedDateTime

import akka.http.scaladsl.marshalling.sse.EventStreamMarshalling._
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.StatusCodes.Found
import akka.http.scaladsl.model.Uri
import akka.http.scaladsl.model.sse.ServerSentEvent
import akka.http.scaladsl.server
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.Source
import io.swagger.annotations.Api
import io.swagger.annotations.ApiImplicitParam
import io.swagger.annotations.ApiImplicitParams
import io.swagger.annotations.ApiOperation
import io.swagger.annotations.ApiParam
import io.swagger.annotations.ApiResponse
import io.swagger.annotations.ApiResponses
import javax.ws.rs.DELETE
import javax.ws.rs.GET
import javax.ws.rs.POST
import javax.ws.rs.PUT
import javax.ws.rs.Path

import com.dimajix.flowman.hub.model.Converter
import com.dimajix.flowman.hub.model.Kernel
import com.dimajix.flowman.hub.model.KernelList
import com.dimajix.flowman.hub.model.KernelLogMessage
import com.dimajix.flowman.hub.service.KernelManager
import com.dimajix.flowman.hub.service.KernelService
import com.dimajix.flowman.hub.service.KernelState
import com.dimajix.flowman.hub.service.LaunchEnvironment
import com.dimajix.flowman.hub.service.LauncherManager


@Api(value = "kernel", produces = "application/json", consumes = "application/json")
@Path("/kernel")
@ApiResponses(Array(
    new ApiResponse(code = 500, message = "Internal Server Error")
))
class KernelEndpoint(kernelManager:KernelManager, launcherManager:LauncherManager) {
    import akka.http.scaladsl.server.Directives._

    import com.dimajix.flowman.hub.model.JsonSupport._

    private val clock = Clock.systemDefaultZone()

    def routes : server.Route = pathPrefix("kernel") {(
        pathEndOrSingleSlash {
            redirectToNoTrailingSlashIfPresent(Found) {(
                createKernel()
                ~
                listKernel()
            )}
        }
        ~
        pathPrefix(Segment) { kernel => (
            pathEndOrSingleSlash {
                redirectToNoTrailingSlashIfPresent(Found) {(
                    getKernel(kernel)
                    ~
                    stopKernel(kernel)
                )}
            }
            ~
            path("log") {
                getKernelLog(kernel)
            }
            ~
            invokeKernel(kernel)
        )}
    )}

    @POST
    @ApiOperation(value = "Launch a new kernel", nickname = "createKernel", httpMethod = "POST")
    @ApiResponses(Array(
        new ApiResponse(code = 200, message = "Create new kernel", response = classOf[Kernel])
    ))
    def createKernel() : server.Route = {
        post {
            val launcher = launcherManager.list().head
            val env = LaunchEnvironment()
            val kernel = kernelManager.launchKernel(launcher, env)
            complete(Converter.of(kernel))
        }
    }

    @GET
    @ApiOperation(value = "List all known kernels", nickname = "listKernels", httpMethod = "GET")
    @ApiResponses(Array(
        new ApiResponse(code = 200, message = "List of kernels", response = classOf[KernelList])
    ))
    def listKernel() : server.Route = {
        get {
            val kernels = kernelManager.list().filter(_.state != KernelState.TERMINATED)
            val result = KernelList(kernels.map(Converter.of))
            complete(result)
        }
    }

    @GET
    @Path("/{kernel}")
    @ApiOperation(value = "Retrieve a specific kernel by its ID", nickname = "getKernel", httpMethod = "GET")
    @ApiImplicitParams(Array(
        new ApiImplicitParam(name = "kernel", value = "Kernel ID", required = true, dataType = "string", paramType = "path")
    ))
    @ApiResponses(Array(
        new ApiResponse(code = 200, message = "Retrieve information about a  specific kernel", response = classOf[Kernel]),
        new ApiResponse(code = 404, message = "Kernel not found")
    ))
    def getKernel(@ApiParam(hidden = true) kernel:String) : server.Route = {
        get {
            withKernel(kernel) { kernel =>
                val result = Converter.of(kernel)
                complete(result)
            }
        }
    }

    @DELETE
    @Path("/{kernel}")
    @ApiOperation(value = "Shutdown a running kernel", nickname = "stopKernel", httpMethod = "DELETE")
    @ApiImplicitParams(Array(
        new ApiImplicitParam(name = "kernel", value = "Kernel ID", required = true, dataType = "string", paramType = "path")
    ))
    @ApiResponses(Array(
        new ApiResponse(code = 200, message = "Successfully stopped kernel", response = classOf[String]),
        new ApiResponse(code = 404, message = "Kernel not found")
    ))
    def stopKernel(@ApiParam(hidden = true) kernel:String) : server.Route = {
        delete {
            withKernel(kernel) { kernel =>
                kernel.shutdown()
                complete("success")
            }
        }
    }

    @GET
    @Path("/{kernel}/log")
    @ApiOperation(value = "Retrieve the kernel log as a SSE stream", nickname = "getKernelLog", httpMethod = "GET")
    @ApiImplicitParams(Array(
        new ApiImplicitParam(name = "kernel", value = "Kernel ID", required = true, dataType = "string", paramType = "path")
    ))
    @ApiResponses(Array(
        new ApiResponse(code = 200, message = "Successfully retrieved kernel log"),
        new ApiResponse(code = 404, message = "Kernel not found")
    ))
    def getKernelLog(@ApiParam(hidden = true) kernel:String) : server.Route = {
        get {
            withKernel(kernel) { kernel =>
                complete {
                    Source.fromPublisher(kernel.messages)
                        .map { message =>
                            val event = KernelLogMessage(kernel.id, ZonedDateTime.now(), message)
                            ServerSentEvent(kernelLogMessageFormat.write(event).toString(), Some("message"))
                        }
                }
            }
        }
    }

    @GET
    @PUT
    @POST
    @DELETE
    @Path("/{kernel}/")
    @ApiOperation(value = "Invoke a kernel", nickname = "invokeKernel", httpMethod = "GET")
    @ApiImplicitParams(Array(
        new ApiImplicitParam(name = "kernel", value = "Kernel ID", required = true, dataType = "string", paramType = "path")
    ))
    @ApiResponses(Array(
        new ApiResponse(code = 200, message = "Successfully invoked kernel"),
        new ApiResponse(code = 404, message = "Kernel not found")
    ))
    def invokeKernel(@ApiParam(hidden = true) kernel:String) : server.Route = {
        withKernel(kernel) { kernel =>
            extractUnmatchedPath { path =>
                extractRequest { request =>
                    val uri = request.uri.withPath(Uri.Path("/api") ++ path)
                    val newRequest = request.withUri(uri)
                    // TODO: Think about redirects etc...
                    complete(kernel.invoke(newRequest))
                }
            }
        }
    }

    private def withKernel(kernel:String)(fn:KernelService => server.Route) : server.Route = {
        kernelManager.getKernel(kernel) match {
            case Some(svc) => fn(svc)
            case None => complete(HttpResponse(status = StatusCodes.NotFound))
        }
    }
}
