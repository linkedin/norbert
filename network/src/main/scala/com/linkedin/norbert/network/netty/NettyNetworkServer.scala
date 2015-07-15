/*
 * Copyright 2009-2010 LinkedIn, Inc
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
package com.linkedin.norbert
package network
package netty

import java.util.concurrent.{TimeUnit, ScheduledExecutorService, ScheduledFuture, Executors}
import org.jboss.netty.bootstrap.ServerBootstrap
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory
import org.jboss.netty.handler.logging.LoggingHandler
import org.jboss.netty.handler.codec.frame.{LengthFieldBasedFrameDecoder, LengthFieldPrepender}
import org.jboss.netty.handler.codec.protobuf.{ProtobufDecoder, ProtobufEncoder}
import org.jboss.netty.channel.group.DefaultChannelGroup
import server._
import com.linkedin.norbert.cluster.{Node, ClusterClient, ClusterClientComponent}
import protos.NorbertProtos
import com.linkedin.norbert.norbertutils.{SystemClockComponent, NamedPoolThreadFactory}
import org.jboss.netty.channel.{Channels, ChannelPipelineFactory}

class NetworkServerConfig {
  var clusterClient: ClusterClient = _
  var serviceName: String = _
  var zooKeeperConnectString: String = _
  var zooKeeperSessionTimeoutMillis = 30000

  var requestTimeoutMillis = NetworkDefaults.REQUEST_TIMEOUT_MILLIS
  var responseGenerationTimeoutMillis = -1//turned off by default

  var requestThreadCorePoolSize = NetworkDefaults.REQUEST_THREAD_CORE_POOL_SIZE
  var requestThreadMaxPoolSize = NetworkDefaults.REQUEST_THREAD_MAX_POOL_SIZE
  var requestThreadKeepAliveTimeSecs = NetworkDefaults.REQUEST_THREAD_KEEP_ALIVE_TIME_SECS

  var threadPoolQueueSize = NetworkDefaults.REQUEST_THREAD_POOL_QUEUE_SIZE

  var requestStatisticsWindow = NetworkDefaults.REQUEST_STATISTICS_WINDOW
  var avoidByteStringCopy = NetworkDefaults.AVOID_BYTESTRING_COPY

  var shutdownPauseMultiplier = NetworkDefaults.SHUTDOWN_PAUSE_MULTIPLIER

  var gcParams = NetworkDefaults.GC_PARAMS
}

class NettyNetworkServer(serverConfig: NetworkServerConfig) extends NetworkServer with ClusterClientComponent with NettyClusterIoServerComponent
    with MessageHandlerRegistryComponent with MessageExecutorComponent with GcDetector with SystemClockComponent {
  val clusterClient = if (serverConfig.clusterClient != null) serverConfig.clusterClient else ClusterClient(serverConfig.serviceName, serverConfig.zooKeeperConnectString,
    serverConfig.zooKeeperSessionTimeoutMillis)

  val messageHandlerRegistry = new MessageHandlerRegistry
  val messageExecutor = new ThreadPoolMessageExecutor(clientName = clusterClient.clientName,
                                                      serviceName = clusterClient.serviceName,
                                                      messageHandlerRegistry = messageHandlerRegistry,
                                                      requestTimeout = serverConfig.requestTimeoutMillis,
                                                      corePoolSize = serverConfig.requestThreadCorePoolSize,
                                                      maxPoolSize = serverConfig.requestThreadMaxPoolSize,
                                                      keepAliveTime = serverConfig.requestThreadKeepAliveTimeSecs,
                                                      maxWaitingQueueSize = serverConfig.threadPoolQueueSize,
                                                      requestStatisticsWindow = serverConfig.requestStatisticsWindow,
                                                      responseGenerationTimeoutMillis = serverConfig.responseGenerationTimeoutMillis,
                                                      gcParams = serverConfig.gcParams,
                                                      myNode = Some(myNode))

  val executor = Executors.newCachedThreadPool(new NamedPoolThreadFactory("norbert-server-pool-%s".format(clusterClient.serviceName)))
  val bootstrap = new ServerBootstrap(new NioServerSocketChannelFactory(executor, executor))
  val channelGroup = new DefaultChannelGroup("norbert-server-group-%s".format(clusterClient.serviceName))
  val requestContextEncoder = new RequestContextEncoder()

  val gcThread: Option[ScheduledExecutorService] = serverConfig.gcParams.enableGcAwareness match {
    case true => Some(Executors.newSingleThreadScheduledExecutor())
    case _ => None
  }
  var gcFuture: Option[ScheduledFuture[_]] = None
  var currOffset: Int = -1
  val gcCycleTime = serverConfig.gcParams.cycleTime
  val gcSlotTime = serverConfig.gcParams.slotTime

  bootstrap.setOption("reuseAddress", true)
  bootstrap.setOption("tcpNoDelay", true)
  bootstrap.setOption("child.tcpNoDelay", true)
  bootstrap.setOption("child.reuseAddress", true)

  val serverFilterChannelHandler = new ServerFilterChannelHandler(messageExecutor)
  val serverChannelHandler = new ServerChannelHandler(
    clientName = clusterClient.clientName,
    serviceName = clusterClient.serviceName,
    channelGroup = channelGroup,
    messageHandlerRegistry = messageHandlerRegistry,
    messageExecutor = messageExecutor,
    requestStatisticsWindow = serverConfig.requestStatisticsWindow,
    avoidByteStringCopy = serverConfig.avoidByteStringCopy)

  bootstrap.setPipelineFactory(new ChannelPipelineFactory {
    val loggingHandler = new LoggingHandler
    val protobufDecoder = new ProtobufDecoder(NorbertProtos.NorbertMessage.getDefaultInstance)
    val requestContextDecoder = new RequestContextDecoder
    val frameEncoder = new LengthFieldPrepender(4)
    val protobufEncoder = new ProtobufEncoder
    val handler = serverChannelHandler

    def getPipeline = {
      val p = Channels.pipeline

      if (log.debugEnabled) p.addFirst("logging", loggingHandler)
      p.addLast("frameDecoder", new LengthFieldBasedFrameDecoder(Int.MaxValue, 0, 4, 0, 4))
      p.addLast("protobufDecoder", protobufDecoder)

      p.addLast("frameEncoder", frameEncoder)
      p.addLast("protobufEncoder", protobufEncoder)

      p.addLast("requestContextDecoder", requestContextDecoder)
      p.addLast("requestContextEncoder", requestContextEncoder)
      p.addLast("requestFilterHandler", serverFilterChannelHandler)
      p.addLast("requestHandler", handler)

      p
    }
  })

  def setRequestTimeoutMillis(newValue : Long) = {
      messageExecutor.setRequestTimeout(newValue)
  }

  val clusterIoServer = new NettyClusterIoServer(bootstrap, channelGroup)

  override def shutdown = {
    if (serverConfig.shutdownPauseMultiplier > 0)
    {
      markUnavailable
      Thread.sleep(serverConfig.shutdownPauseMultiplier * serverConfig.zooKeeperSessionTimeoutMillis)
    }

    if (serverConfig.clusterClient == null) clusterClient.shutdown else super.shutdown
    //change the sequence so that we do not accept any more connections from clients
    //are existing connections could feed us new norbert messages
    serverChannelHandler.shutdown
    messageExecutor.shutdown
    if (gcThread.isDefined)
      gcThread.get.shutdown
//    requestContextEncoder.shutdown
  }

  //Need to override bindNode to setup periodic GC event based on Node offset.
  //Hooks to the request queue don't work as the queue may not come into the picture (see how ArrayBlockingQueues work)
  //Pre-execute hooks don't work since the last one may occur before the node's GC slot
  //Post-execute hooks don't work because of the latter, and because a long-running request
  //may hog up the entire slot.
  override def bindNode(node: Node, markAvailable: Boolean, initialCapability: Long = 0L): Unit = {

    super.bindNode(node, markAvailable, initialCapability)

    if(serverConfig.gcParams.enableGcAwareness) {

      schedulePeriodicGc(node)

    }

  }

  def schedulePeriodicGc(node: Node): Unit = {

    if (node.offset.isEmpty) {
      log.error("Registering node " + node.id + " without an offset, even though GC awareness parameters are present")
      if (gcFuture.isDefined) {
        // Cancel the current event, if there is one
        log.info("Cancelling the periodic GC")
        gcFuture.get.cancel(true)
      }
      return
    }

    log.info("Registering: " + node.id + " with offset: " + node.offset.get)

    //Check if there is already a periodic GC event running
    if (gcFuture.isDefined) {
      //Check if the already running GC event occurs at the same offset as that required by the new node
      if (node.offset.get != currOffset) {
        //It doesn't, cancel the current event
        gcFuture.get.cancel(true)
      }
      else {
        return
      }
    }

    //Schedule the new periodic GC event on the gcThread.
    gcFuture = Some(
      gcThread.get.scheduleAtFixedRate(new GC(Some(messageExecutor.getThreadPoolStats _)), timeTillNextGC(node.offset.get) + getGcParams.slaTime,
        getGcParams.cycleTime, TimeUnit.MILLISECONDS))
    currOffset = node.offset.get
  }

  def getGcParams:GcParamWrapper = serverConfig.gcParams
}
