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

import org.jboss.netty.bootstrap.ClientBootstrap
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory
import org.jboss.netty.handler.logging.LoggingHandler
import org.jboss.netty.handler.codec.frame.{LengthFieldBasedFrameDecoder, LengthFieldPrepender}
import org.jboss.netty.handler.codec.protobuf.{ProtobufDecoder, ProtobufEncoder}
import java.util.concurrent.Executors
import partitioned.loadbalancer.{PartitionedLoadBalancerFactoryComponent, PartitionedLoadBalancerFactory}
import partitioned.PartitionedNetworkClient
import client.loadbalancer.{LoadBalancerFactoryComponent, LoadBalancerFactory}
import com.linkedin.norbert.cluster.{Node, ClusterClient, ClusterClientComponent}
import protos.NorbertProtos
import org.jboss.netty.channel.{ChannelPipelineFactory, Channels}
import client.{ThreadPoolResponseHandler, ResponseHandlerComponent, NetworkClient, NetworkClientConfig}
import com.linkedin.norbert.network.common.{CachedNetworkStatistics, CompositeCanServeRequestStrategy, SimpleBackoffStrategy, BaseNetworkClient}
import java.util.{Map => JMap, UUID}
import jmx.JMX
import jmx.JMX.MBean
import norbertutils._

abstract class BaseNettyNetworkClient(clientConfig: NetworkClientConfig) extends BaseNetworkClient with ClusterClientComponent with NettyClusterIoClientComponent with ResponseHandlerComponent {
  val clusterClient = if (clientConfig.clusterClient != null) clientConfig.clusterClient else ClusterClient(clientConfig.clientName, clientConfig.serviceName, clientConfig.zooKeeperConnectString,
    clientConfig.zooKeeperSessionTimeoutMillis)

  private val executor = Executors.newCachedThreadPool(new NamedPoolThreadFactory("norbert-client-pool-%s".format(clusterClient.serviceName)))
  private val bootstrap = new ClientBootstrap(new NioClientSocketChannelFactory(executor, executor))
  private val connectTimeoutMillis = clientConfig.connectTimeoutMillis
  private val receiveBufferSize = clientConfig.receiveBufferSize
  private val sendBufferSize = clientConfig.sendBufferSize

  val responseHandler = new ThreadPoolResponseHandler(
    clientName = clusterClient.clientName,
    serviceName = clusterClient.serviceName,
    corePoolSize = clientConfig.responseHandlerCorePoolSize,
    maxPoolSize = clientConfig.responseHandlerMaxPoolSize,
    keepAliveTime = clientConfig.responseHandlerKeepAliveTime,
    maxWaitingQueueSize = clientConfig.responseHandlerMaxWaitingQueueSize,
    avoidByteStringCopy = clientConfig.avoidByteStringCopy)

  private val stats = CachedNetworkStatistics[Node, UUID](SystemClock, clientConfig.requestStatisticsWindow, 200L)

  private val handler = new ClientChannelHandler(
    clientName = clusterClient.clientName,
    serviceName = clusterClient.serviceName,
    staleRequestTimeoutMins = clientConfig.staleRequestTimeoutMins,
    staleRequestCleanupFrequencyMins= clientConfig.staleRequestCleanupFrequenceMins,
    requestStatisticsWindow = clientConfig.requestStatisticsWindow,
    outlierMultiplier = clientConfig.outlierMuliplier,
    outlierConstant = clientConfig.outlierConstant,
    responseHandler = responseHandler,
    avoidByteStringCopy = clientConfig.avoidByteStringCopy,
    stats = stats,
    routeAway = clientConfig.routingAwayCallback)

  private val darkCanaryHandler = new DarkCanaryChannelHandler()

  // TODO why isn't clientConfig visible here?
  bootstrap.setOption("connectTimeoutMillis", connectTimeoutMillis)
  bootstrap.setOption("tcpNoDelay", true)
  bootstrap.setOption("reuseAddress", true)
  if (receiveBufferSize > 0) {
    bootstrap.setOption("receiveBufferSize", receiveBufferSize)
  }
  if (sendBufferSize > 0) {
    bootstrap.setOption("sendBufferSize", sendBufferSize)
  }
  bootstrap.setPipelineFactory(new ChannelPipelineFactory {
    private val loggingHandler = new LoggingHandler
    private val protobufDecoder = new ProtobufDecoder(NorbertProtos.NorbertMessage.getDefaultInstance)
    private val darkCanaryDownstreamHandler = new darkCanaryHandler.DownStreamHandler()
    private val darkCanaryUpstreamHandler = new darkCanaryHandler.UpstreamHandler()
    private val frameEncoder = new LengthFieldPrepender(4)
    private val protobufEncoder = new ProtobufEncoder

    def getPipeline = {
      val p = Channels.pipeline

      p.addFirst("logging", loggingHandler)

      p.addLast("frameDecoder", new LengthFieldBasedFrameDecoder(Int.MaxValue, 0, 4, 0, 4))
      p.addLast("protobufDecoder", protobufDecoder)

      p.addLast("frameEncoder", frameEncoder)
      p.addLast("protobufEncoder", protobufEncoder)

      clientConfig.darkCanaryServiceName match {
        case Some(serviceName) => p.addLast("darkCanaryUpstreamHandler", darkCanaryUpstreamHandler)
        case None =>  // Do nothing. We register dark canary handlers only if a dark canary service name is specified.
      }

      p.addLast("requestHandler", handler)

      clientConfig.darkCanaryServiceName match {
        case Some(serviceName) => p.addLast("darkDownstreamCanaryHandler", darkCanaryDownstreamHandler)
        case None =>  // Do nothing. We register dark canary handlers only if a dark canary service name is specified.
      }
      p
    }
  })

  trait EndpointStatusMBean {
    def getEndpoints: JMap[Int, Boolean]

    def getNumNodesDown: Int
  }

  private val endpointsJMX = JMX.register(new MBean(classOf[EndpointStatusMBean], JMX.name(clusterClient.clientName, clusterClient.serviceName))
          with EndpointStatusMBean {
    def getEndpoints = {
      toJMap(endpoints.map{e => (e.node.id, e.canServeRequests)}.toMap)
    }

    def getNumNodesDown = endpoints.filter(e => !e.canServeRequests).size
  })

  val channelPoolStrategy = new SimpleBackoffStrategy(SystemClock)
  val clientChannelStrategy = handler.strategy // TODO: Carefully consider making this strategy a constructor for the ClientChannelHandler

  val strategy = CompositeCanServeRequestStrategy(channelPoolStrategy, clientChannelStrategy)

  val channelPoolFactory = new ChannelPoolFactory(maxConnections = clientConfig.maxConnectionsPerNode,
    openTimeoutMillis = clientConfig.connectTimeoutMillis,
    writeTimeoutMillis = clientConfig.writeTimeoutMillis,
    bootstrap = bootstrap,
    closeChannelTimeMillis = clientConfig.closeChannelTimeMillis,
    staleRequestTimeoutMins = clientConfig.staleRequestTimeoutMins + 1,
    staleRequestCleanupFreqMins = clientConfig.staleRequestCleanupFrequenceMins,
    errorStrategy = Some(channelPoolStrategy),
    stats = stats)

  val clusterIoClient = new NettyClusterIoClient(channelPoolFactory, strategy)
  clientConfig.darkCanaryServiceName match {
    case Some(serviceName) => darkCanaryHandler.initialize(clientConfig, clusterIoClient)
    case None => // Do nothing. We initialize dark canaries only if a dark canary service name is specified.
  }


  override def shutdown = {
    if (clientConfig.clusterClient == null) clusterClient.shutdown else super.shutdown
    handler.shutdown
    endpointsJMX.foreach(JMX.unregister(_))
  }
}

class NettyNetworkClient(clientConfig: NetworkClientConfig, val loadBalancerFactory: LoadBalancerFactory) extends BaseNettyNetworkClient(clientConfig) with NetworkClient with LoadBalancerFactoryComponent

class NettyPartitionedNetworkClient[PartitionedId](clientConfig: NetworkClientConfig, val loadBalancerFactory: PartitionedLoadBalancerFactory[PartitionedId]) extends BaseNettyNetworkClient(clientConfig)
    with PartitionedNetworkClient[PartitionedId] with PartitionedLoadBalancerFactoryComponent[PartitionedId] {
  setConfig(clientConfig)
}
