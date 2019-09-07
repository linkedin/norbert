package com.linkedin.norbert
package network
package netty

import java.net.SocketAddress
import java.util.UUID

import com.linkedin.norbert.cluster.Node
import com.linkedin.norbert.network.client.ResponseHandler
import com.linkedin.norbert.network.common.{CachedNetworkStatistics, SampleMessage}
import com.linkedin.norbert.norbertutils.MockClock
import com.linkedin.norbert.protos.NorbertProtos
import com.linkedin.norbert.protos.NorbertProtos.NorbertMessage.Status
import org.jboss.netty.channel._
import org.specs2.mock.Mockito
import org.specs2.mutable.SpecificationWithJUnit
import org.specs2.specification.Scope

/**
  * Test to cover association of RequestAccess with remote exception
  */
class ClientChannelHandlerSpec extends SpecificationWithJUnit with Mockito with SampleMessage {

  trait ClientChannelHandlerSetup extends Scope {
    val responseHandler = mock[ResponseHandler]
    val mockClock = new MockClock
    val statsActor = CachedNetworkStatistics[Node, UUID](mockClock, 1000L, 200L)
    val clientChannelHandler = new ClientChannelHandler(clientName = Some("booClient"),
      serviceName = "booService",
      staleRequestTimeoutMins = 3000,
      staleRequestCleanupFrequencyMins = 3000,
      requestStatisticsWindow = 3000L,
      outlierMultiplier = 2,
      outlierConstant = 2,
      responseHandler = responseHandler,
      avoidByteStringCopy = true,
      stats = statsActor,
      enableReroutingStrategies = true
    )

    def sendMockRequest(ctx: ChannelHandlerContext, request: Request[Ping, Ping]) {
      val writeEvent = mock[MessageEvent]
      writeEvent.getChannel returns ctx.getChannel
      val channelFuture = mock[ChannelFuture]
      writeEvent.getFuture returns channelFuture
      writeEvent.getMessage returns request
      clientChannelHandler.writeRequested(ctx, writeEvent)
    }
  }

  "ClientChannelHandler" should {
    "throw exception with RequestAccess when server response is HEAVYLOAD" in new ClientChannelHandlerSetup {
      val channel = mock[Channel]
      channel.getRemoteAddress returns mock[SocketAddress]
      val ctx = mock[ChannelHandlerContext]
      ctx.getChannel returns channel
      val request = Request[Ping, Ping](Ping(System.currentTimeMillis), Node(1, "localhost:1234", true), Ping.PingSerializer, Ping.PingSerializer, Some({ e => e }))
      sendMockRequest(ctx, request)

      val readEvent = mock[MessageEvent]
      val norbertMessage = NorbertProtos.NorbertMessage.newBuilder().setStatus(Status.HEAVYLOAD)
        .setRequestIdLsb(request.id.getLeastSignificantBits)
        .setRequestIdMsb(request.id.getMostSignificantBits)
        .setMessageName("Boo")
        .build
      readEvent.getMessage returns norbertMessage
      clientChannelHandler.messageReceived(ctx, readEvent)
      there was one(responseHandler).onFailure(any[Request[_, _]], any[Throwable with RequestAccess[Request[_, _]]])
    }

    "throw exception with RequestAccess when server response is ERROR" in new ClientChannelHandlerSetup {
      val channel = mock[Channel]
      channel.getRemoteAddress returns mock[SocketAddress]
      val ctx = mock[ChannelHandlerContext]
      ctx.getChannel returns channel
      val request = Request[Ping, Ping](Ping(System.currentTimeMillis), Node(1, "localhost:1234", true), Ping.PingSerializer, Ping.PingSerializer, Some({ e => e }))
      sendMockRequest(ctx, request)

      val norbertMessage = NorbertProtos.NorbertMessage.newBuilder().setStatus(Status.ERROR)
        .setRequestIdLsb(request.id.getLeastSignificantBits)
        .setRequestIdMsb(request.id.getMostSignificantBits)
        .setMessageName("Boo")
        .setErrorMessage("BooBoo")
        .build
      val readEvent = mock[MessageEvent]
      readEvent.getMessage returns norbertMessage
      clientChannelHandler.messageReceived(ctx, readEvent)
      there was one(responseHandler).onFailure(any[Request[_, _]], any[Throwable with RequestAccess[Request[_, _]]])
    }
  }
}