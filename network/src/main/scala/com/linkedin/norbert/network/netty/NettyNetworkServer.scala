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

import java.util.concurrent.Executors
import org.jboss.netty.bootstrap.ServerBootstrap
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory
import org.jboss.netty.handler.logging.LoggingHandler
import org.jboss.netty.handler.codec.frame.{LengthFieldBasedFrameDecoder, LengthFieldPrepender}
import org.jboss.netty.handler.codec.protobuf.{ProtobufDecoder, ProtobufEncoder}
import org.jboss.netty.channel.group.DefaultChannelGroup
import server._
import cluster.{ClusterClient, ClusterClientComponent}
import protos.NorbertProtos
import norbertutils.NamedPoolThreadFactory
import org.jboss.netty.channel.{Channels, ChannelPipelineFactory}

import scala.beans.BeanProperty

class NetworkServerConfig {
  @BeanProperty var clusterClient: ClusterClient = _
  @BeanProperty var serviceName: String = _
  @BeanProperty var zooKeeperConnectString: String = _
  @BeanProperty var zooKeeperSessionTimeoutMillis = 30000

  @BeanProperty var requestTimeoutMillis = NetworkDefaults.REQUEST_TIMEOUT_MILLIS
  @BeanProperty var responseGenerationTimeoutMillis = -1//turned off by default

  @BeanProperty var requestThreadCorePoolSize = NetworkDefaults.REQUEST_THREAD_CORE_POOL_SIZE
  @BeanProperty var requestThreadMaxPoolSize = NetworkDefaults.REQUEST_THREAD_MAX_POOL_SIZE
  @BeanProperty var requestThreadKeepAliveTimeSecs = NetworkDefaults.REQUEST_THREAD_KEEP_ALIVE_TIME_SECS

  @BeanProperty var threadPoolQueueSize = NetworkDefaults.REQUEST_THREAD_POOL_QUEUE_SIZE

  @BeanProperty var requestStatisticsWindow = NetworkDefaults.REQUEST_STATISTICS_WINDOW
  @BeanProperty var avoidByteStringCopy = NetworkDefaults.AVOID_BYTESTRING_COPY

  @BeanProperty var shutdownPauseMultiplier = NetworkDefaults.SHUTDOWN_PAUSE_MULTIPLIER
}

class NettyNetworkServer(serverConfig: NetworkServerConfig) extends NetworkServer with ClusterClientComponent with NettyClusterIoServerComponent
    with MessageHandlerRegistryComponent with MessageExecutorComponent {
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
                                                      responseGenerationTimeoutMillis = serverConfig.responseGenerationTimeoutMillis)

  val executor = Executors.newCachedThreadPool(new NamedPoolThreadFactory("norbert-server-pool-%s".format(clusterClient.serviceName)))
  val bootstrap = new ServerBootstrap(new NioServerSocketChannelFactory(executor, executor))
  val channelGroup = new DefaultChannelGroup("norbert-server-group-%s".format(clusterClient.serviceName))
  val requestContextEncoder = new RequestContextEncoder()

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
//    requestContextEncoder.shutdown
  }
}
