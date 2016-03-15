/*
 * Copyright 2014- LinkedIn, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.linkedin.norbert.network.netty

import org.specs.SpecificationWithJUnit
import com.linkedin.norbert.network.common.{CanServeRequestStrategy, ClusterIoClientComponent, SampleMessage}
import com.linkedin.norbert.cluster.Node
import com.linkedin.norbert.network.client.NetworkClientConfig
import org.jboss.netty.channel.{ChannelFuture, MessageEvent, ChannelHandlerContext, Channel}
import java.net.SocketAddress
import com.linkedin.norbert.network.Request
import org.specs.mock.Mockito
import com.linkedin.norbert.protos.NorbertProtos
import com.linkedin.norbert.protos.NorbertProtos.NorbertMessage.Status

/**
 * Test mirroring of messages and the successful dropping of mirrored messages.
 */
class DarkCanaryChannelHandlerSpec extends SpecificationWithJUnit with Mockito with SampleMessage {

  val mockClusterIoClient = mock[ClusterIoClientComponent#ClusterIoClient]
  val darkCanaryHandler= new DarkCanaryChannelHandler()
  val clientConfig = new NetworkClientConfig
  clientConfig.clientName = "foobar"
  darkCanaryHandler.initialize(clientConfig, mockClusterIoClient, new CanServeRequestStrategy {
    override def canServeRequest(node: Node): Boolean = return "localhost:1235".equals(node.url)
  })

  def getMessageEvent(ctx: ChannelHandlerContext, request: Request[Ping, Ping]) : MessageEvent = {
    val writeEvent = mock[MessageEvent]
    writeEvent.getChannel returns ctx.getChannel
    val channelFuture = mock[ChannelFuture]
    writeEvent.getFuture returns channelFuture
    writeEvent.getMessage returns request
    writeEvent
  }


  // Writes a request to the handler with Node.id == 1. All the tests rely on this fact.
  def writeRequest(handler : DarkCanaryChannelHandler#DownStreamHandler) : (MessageEvent, ChannelHandlerContext) = {
    val channel = mock[Channel]
    channel.getRemoteAddress returns mock[SocketAddress]
    val ctx = mock[ChannelHandlerContext]
    ctx.getChannel returns channel
    val request = Request[Ping, Ping](Ping(System.currentTimeMillis),
      Node(1, "localhost:1234", true),
      Ping.PingSerializer,
      Ping.PingSerializer,
      None)
    val writeEvent = getMessageEvent(ctx, request)
    handler.writeRequested(ctx, writeEvent)
    (writeEvent, ctx)
  }

  "DownstreamHandler" should {
    "not mirror request when there is no dark canary configuration" in {
      val downstreamHandler = new darkCanaryHandler.DownStreamHandler
      writeRequest(downstreamHandler)
      darkCanaryHandler.getInFlightRequestIds.size mustEq 0
      org.mockito.Mockito.verify(mockClusterIoClient, org.mockito.Mockito.times(0))
        .sendMessage(any[Node], any[Request[Ping,Ping]])
    }

    "not mirror request when incoming node and configured nodes do not match" in {
      val downstreamHandler = new darkCanaryHandler.DownStreamHandler
      darkCanaryHandler.addNode(Node(2, "localhost:1235", true))
      writeRequest(downstreamHandler)
      darkCanaryHandler.getInFlightRequestIds.size mustEq 0
      org.mockito.Mockito.verify(mockClusterIoClient, org.mockito.Mockito.times(0))
        .sendMessage(any[Node], any[Request[Ping,Ping]])
    }

    "not mirror request when the dark host cannot serve request" in {
      val downstreamHandler = new darkCanaryHandler.DownStreamHandler
      darkCanaryHandler.addNode(Node(1, "localhost:1236", true))
      writeRequest(downstreamHandler)
      darkCanaryHandler.getInFlightRequestIds.size mustEq 0
      org.mockito.Mockito.verify(mockClusterIoClient, org.mockito.Mockito.times(0))
        .sendMessage(any[Node], any[Request[Ping,Ping]])
    }

    "mirror request when incoming node and configured node ids match" in {
      val downstreamHandler = new darkCanaryHandler.DownStreamHandler
      darkCanaryHandler.addNode(Node(1, "localhost:1235", true))
      writeRequest(downstreamHandler)
      darkCanaryHandler.getInFlightRequestIds.size mustEq 1
      org.mockito.Mockito.verify(mockClusterIoClient, org.mockito.Mockito.times(1))
        .sendMessage(any[Node], any[Request[Ping,Ping]])
    }
  }

  "UpstreamHandler" should {
    "drop mirrored responses" in {
      val downstreamHandler = new darkCanaryHandler.DownStreamHandler
      darkCanaryHandler.addNode(Node(1, "localhost:1235", true))
      val (event, ctx) = writeRequest(downstreamHandler)
      val readEvent = mock[MessageEvent]
      darkCanaryHandler.getInFlightRequestIds.size mustEq 1
      val mirroredRequestId = darkCanaryHandler.getInFlightRequestIds(0)
      val norbertMessage = NorbertProtos.NorbertMessage.newBuilder().setStatus(Status.OK)
        .setRequestIdLsb(mirroredRequestId.getLeastSignificantBits)
        .setRequestIdMsb(mirroredRequestId.getMostSignificantBits)
        .setMessageName("Boo")
        .build
      readEvent.getMessage returns norbertMessage

      val upstreamHandler = new darkCanaryHandler.UpstreamHandler
      upstreamHandler.messageReceived(ctx, readEvent)
      org.mockito.Mockito.verify(ctx, org.mockito.Mockito.times(0)).sendUpstream(any[MessageEvent])
    }

    "propagate responses when there is no dark canary configuration" in {
      val downstreamHandler = new darkCanaryHandler.DownStreamHandler
      val (event, ctx) = writeRequest(downstreamHandler)
      darkCanaryHandler.getInFlightRequestIds.size mustEq 0
      val request = event.getMessage.asInstanceOf[Request[Ping, Ping]]
      val readEvent = mock[MessageEvent]
      val norbertMessage = NorbertProtos.NorbertMessage.newBuilder().setStatus(Status.OK)
        .setRequestIdLsb(request.id.getLeastSignificantBits)
        .setRequestIdMsb(request.id.getMostSignificantBits)
        .setMessageName("Boo")
        .build
      readEvent.getMessage returns norbertMessage

      val upstreamHandler = new darkCanaryHandler.UpstreamHandler
      upstreamHandler.messageReceived(ctx, readEvent)
      org.mockito.Mockito.verify(ctx, org.mockito.Mockito.times(1)).sendUpstream(readEvent)
    }


    "propagate responses for non mirrored requests" in {
      val downstreamHandler = new darkCanaryHandler.DownStreamHandler
      darkCanaryHandler.addNode(Node(2, "localhost:1235", true))
      val (event, ctx) = writeRequest(downstreamHandler)
      darkCanaryHandler.getInFlightRequestIds.size mustEq 0
      val request = event.getMessage.asInstanceOf[Request[Ping, Ping]]
      val readEvent = mock[MessageEvent]
      val norbertMessage = NorbertProtos.NorbertMessage.newBuilder().setStatus(Status.OK)
        .setRequestIdLsb(request.id.getLeastSignificantBits)
        .setRequestIdMsb(request.id.getMostSignificantBits)
        .setMessageName("Boo")
        .build
      readEvent.getMessage returns norbertMessage

      val upstreamHandler = new darkCanaryHandler.UpstreamHandler
      upstreamHandler.messageReceived(ctx, readEvent)
      org.mockito.Mockito.verify(ctx, org.mockito.Mockito.times(1)).sendUpstream(readEvent)
    }

    "not throw exceptions when the mirrored response is not OK" in {
      val downstreamHandler = new darkCanaryHandler.DownStreamHandler
      darkCanaryHandler.addNode(Node(1, "localhost:1235", true))
      val (event, ctx) = writeRequest(downstreamHandler)
      darkCanaryHandler.getInFlightRequestIds.size mustEq 1
      val mirroredRequestId = darkCanaryHandler.getInFlightRequestIds(0)
      val readEvent = mock[MessageEvent]
      val norbertMessage = NorbertProtos.NorbertMessage.newBuilder().setStatus(Status.HEAVYLOAD)
        .setRequestIdLsb(mirroredRequestId.getLeastSignificantBits)
        .setRequestIdMsb(mirroredRequestId.getMostSignificantBits)
        .setMessageName("Boo")
        .build
      readEvent.getMessage returns norbertMessage

      val upstreamHandler = new darkCanaryHandler.UpstreamHandler
      upstreamHandler.messageReceived(ctx, readEvent)
      org.mockito.Mockito.verify(ctx, org.mockito.Mockito.times(0)).sendUpstream(any[MessageEvent])
    }
  }
}
