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
package partitioned

import java.util.concurrent.Future
import common._
import loadbalancer.{PartitionedLoadBalancer, PartitionedLoadBalancerFactoryComponent, PartitionedLoadBalancerFactory}
import server.{MessageExecutorComponent, NetworkServer}
import netty.NettyPartitionedNetworkClient
import com.linkedin.norbert.network.client.NetworkClientConfig
import cluster.{Node, ClusterDisconnectedException, InvalidClusterException, ClusterClientComponent}
import scala.util.Random
import java.util
import com.linkedin.norbert.network.javaobjects.{NodeSpecification => JNodeSpecification, PartitionedNodeSpecification => JPartitionedNodeSpecification,
RetrySpecification => JRetrySpecification, PartitionedRetrySpecification => JPartitionedRetrySpecification,
RequestSpecification => JRequestSpecification, PartitionedRequestSpecification => JPartitionedRequestSpecification}

object PartitionedNetworkClient {
  def apply[PartitionedId](config: NetworkClientConfig, loadBalancerFactory: PartitionedLoadBalancerFactory[PartitionedId]): PartitionedNetworkClient[PartitionedId] = {
    val nc = new NettyPartitionedNetworkClient(config, loadBalancerFactory)
    nc.start
    nc
  }

  def apply[PartitionedId](config: NetworkClientConfig, loadBalancerFactory: PartitionedLoadBalancerFactory[PartitionedId],
      server: NetworkServer): PartitionedNetworkClient[PartitionedId] = {
    val nc = new NettyPartitionedNetworkClient(config, loadBalancerFactory) with LocalMessageExecution with MessageExecutorComponent {
      val messageExecutor = server.asInstanceOf[MessageExecutorComponent].messageExecutor
      val myNode = server.myNode
    }
    nc.start
    nc
  }
}

/**
 * The network client interface for interacting with nodes in a partitioned cluster.
 */
trait PartitionedNetworkClient[PartitionedId] extends BaseNetworkClient {

  this: ClusterClientComponent with ClusterIoClientComponent  with PartitionedLoadBalancerFactoryComponent[PartitionedId] =>

  var duplicatesOk:Boolean = false
  var retryStrategy:Option[RetryStrategy] = None
  def setConfig(config:NetworkClientConfig): Unit = {
    duplicatesOk = config.duplicatesOk
    if(retryStrategy != null)
      retryStrategy = config.retryStrategy
  }

  @volatile private var loadBalancer: Option[Either[InvalidClusterException, PartitionedLoadBalancer[PartitionedId]]] = None

  @deprecated("Use sendRequest(PartitionedRequestSpecification[RequestMsg, PartitionedId], PartitionedNodeSpecification[PartitionedId], PartitionedRetrySpecification[ResponseMsg], responseAggregator), 2/2/2015")
  def sendRequest[RequestMsg, ResponseMsg](id: PartitionedId, request: RequestMsg, callback: Either[Throwable, ResponseMsg] => Unit)
  (implicit is: InputSerializer[RequestMsg, ResponseMsg], os: OutputSerializer[RequestMsg, ResponseMsg]): Unit =
    {
      val requestSpec = PartitionedRequestSpecification[RequestMsg, PartitionedId](Some(request))
      val nodeSpec = new PartitionedNodeSpecification[PartitionedId](Set(id)).build
      val retrySpec = PartitionedRetrySpecification(callback = Some(callback))
      sendRequest(requestSpec, nodeSpec, retrySpec)
    }

  @deprecated("Use sendRequest(PartitionedRequestSpecification[RequestMsg, PartitionedId], PartitionedNodeSpecification[PartitionedId], PartitionedRetrySpecification[ResponseMsg], responseAggregator), 2/2/2015")
  def sendRequest[RequestMsg, ResponseMsg](id: PartitionedId, request: RequestMsg, callback: Either[Throwable, ResponseMsg] => Unit, capability: Option[Long])
  (implicit is: InputSerializer[RequestMsg, ResponseMsg], os: OutputSerializer[RequestMsg, ResponseMsg]): Unit =
   {
     val requestSpec = PartitionedRequestSpecification[RequestMsg, PartitionedId](Some(request))
     val nodeSpec = new PartitionedNodeSpecification[PartitionedId](Set(id)).setCapability(capability).build
     val retrySpec = PartitionedRetrySpecification(callback = Some(callback))
     sendRequest(requestSpec, nodeSpec, retrySpec)
   }

  @deprecated("Use sendRequest(PartitionedRequestSpecification[RequestMsg, PartitionedId], PartitionedNodeSpecification[PartitionedId], PartitionedRetrySpecification[ResponseMsg], responseAggregator), 2/2/2015")
  def sendRequest[RequestMsg, ResponseMsg](id: PartitionedId, request: RequestMsg, callback: Either[Throwable, ResponseMsg] => Unit, capability: Option[Long], persistentCapability: Option[Long])
  (implicit is: InputSerializer[RequestMsg, ResponseMsg], os: OutputSerializer[RequestMsg, ResponseMsg]): Unit = doIfConnected {
    val requestSpec = PartitionedRequestSpecification[RequestMsg, PartitionedId](Some(request))
    val nodeSpec = new PartitionedNodeSpecification[PartitionedId](Set(id)).setCapability(capability).setPersistentCapability(persistentCapability).build
    val retrySpec = PartitionedRetrySpecification(callback = Some(callback))
    sendRequest(requestSpec, nodeSpec, retrySpec)
  }


  /**
   * Sends a one way <code>Message</code> to the specified <code>PartitionedId</code>. The <code>PartitionedNetworkClient</code>
   * will interact with the current <code>PartitionedLoadBalancer</code> to calculate which <code>Node</code> the message
   * must be sent to.
   *
   * @param ids (or id) the <code>PartitionedId</code> to which the message is addressed
   * @param request the message to send
   *
   * @throws InvalidClusterException thrown if the cluster is currently in an invalid state
   * @throws NoNodesAvailableException thrown if the <code>PartitionedLoadBalancer</code> was unable to provide a <code>Node</code>
   * to send the request to
   * @throws ClusterDisconnectedException thrown if the <code>PartitionedNetworkClient</code> is not connected to the cluster
   */

  def sendMessage[RequestMsg, ResponseMsg](ids: Set[PartitionedId], request: RequestMsg)
                                          (implicit is: InputSerializer[RequestMsg, ResponseMsg], os: OutputSerializer[RequestMsg, ResponseMsg]) {
    doIfConnected {
      sendMessage(ids, (node: Node, ids: Set[PartitionedId]) => request, None, None)(is, os)
    }
  }

  def sendMessage[RequestMsg, ResponseMsg](ids: Set[PartitionedId], request: RequestMsg, capability: Option[Long])
                                          (implicit is: InputSerializer[RequestMsg, ResponseMsg], os: OutputSerializer[RequestMsg, ResponseMsg]) {
    doIfConnected {
      sendMessage(ids, (node: Node, ids: Set[PartitionedId]) => request, capability, None)(is, os)
    }
  }

  def sendMessage[RequestMsg, ResponseMsg](ids: Set[PartitionedId], request: RequestMsg, capability: Option[Long], persistentCapability: Option[Long])
                                          (implicit is: InputSerializer[RequestMsg, ResponseMsg], os: OutputSerializer[RequestMsg, ResponseMsg]) {
    doIfConnected {
      sendMessage(ids, (node: Node, ids: Set[PartitionedId]) => request, capability, persistentCapability)(is, os)
    }
  }

  def sendMessage[RequestMsg, ResponseMsg](ids: Set[PartitionedId], requestBuilder: (Node, Set[PartitionedId]) => RequestMsg, capability: Option[Long])
                                          (implicit is: InputSerializer[RequestMsg, ResponseMsg], os: OutputSerializer[RequestMsg, ResponseMsg]) {
    doIfConnected {
      sendMessage(ids, requestBuilder, capability, None)(is, os)
    }
  }

  def sendMessage[RequestMsg, ResponseMsg](ids: Set[PartitionedId], requestBuilder: (Node, Set[PartitionedId]) => RequestMsg)
                                          (implicit is: InputSerializer[RequestMsg, ResponseMsg], os: OutputSerializer[RequestMsg, ResponseMsg]) {
    doIfConnected {
      sendMessage(ids, requestBuilder, None, None)(is, os)
    }
  }

  def sendMessage[RequestMsg, ResponseMsg](ids: Set[PartitionedId], requestBuilder: (Node, Set[PartitionedId]) => RequestMsg, capability: Option[Long], persistentCapability: Option[Long])
                                          (implicit is: InputSerializer[RequestMsg, ResponseMsg], os: OutputSerializer[RequestMsg, ResponseMsg]) {
    doIfConnected {
      if (ids == null || requestBuilder == null) throw new NullPointerException
      val nodes = calculateNodesFromIds(ids, capability, persistentCapability)
      nodes.foreach {
        case (node, idsForNode) =>
          doSendRequest(PartitionedRequest(requestBuilder(node, idsForNode), node, idsForNode, requestBuilder, is, os, None))
      }
    }
  }

  def sendMessage[RequestMsg, ResponseMsg](id: PartitionedId, request: RequestMsg, capability: Option[Long] = None, persistentCapability: Option[Long] = None)
                              (implicit is: InputSerializer[RequestMsg, ResponseMsg], os: OutputSerializer[RequestMsg, ResponseMsg]) {
    doIfConnected {
      if (id == null || request == null) throw new NullPointerException
      val node = loadBalancer.getOrElse(throw new ClusterDisconnectedException).fold(ex => throw ex, lb => lb.nextNode(id, capability, persistentCapability).getOrElse(throw new NoNodesAvailableException("Unable to satisfy request, no node available for id %s".format(id))))

      doSendRequest(PartitionedRequest(request, node, Set(id), (node: Node, ids: Set[PartitionedId]) => request, is, os, None))
    }
  }

  /**
   * Sends a <code>Message</code> to the specified <code>PartitionedId</code>. The <code>PartitionedNetworkClient</code>
   * will interact with the current <code>PartitionedLoadBalancer</code> to calculate which <code>Node</code> the message
   * must be sent to.  This method is asynchronous and will return immediately.
   *
   * @param id the <code>PartitionedId</code> to which the message is addressed
   * @param request the request to send
   *
   * @return a future which will become available when a response to the message is received
   * @throws InvalidClusterException thrown if the cluster is currently in an invalid state
   * @throws NoNodesAvailableException thrown if the <code>PartitionedLoadBalancer</code> was unable to provide a <code>Node</code>
   * to send the request to
   * @throws ClusterDisconnectedException thrown if the <code>PartitionedNetworkClient</code> is not connected to the cluster
   */
  @deprecated("Use sendRequest(PartitionedRequestSpecification[RequestMsg, PartitionedId], PartitionedNodeSpecification[PartitionedId], PartitionedRetrySpecification[ResponseMsg], responseAggregator), 2/2/2015")
  def sendRequest[RequestMsg, ResponseMsg](id: PartitionedId, request: RequestMsg)
  (implicit is: InputSerializer[RequestMsg, ResponseMsg], os: OutputSerializer[RequestMsg, ResponseMsg]): Future[ResponseMsg] =
  {
    val future = new FutureAdapterListener[ResponseMsg]

    val requestSpec = PartitionedRequestSpecification[RequestMsg, PartitionedId](Some(request))
    val nodeSpec = new PartitionedNodeSpecification[PartitionedId](Set(id)).build
    val retrySpec = PartitionedRetrySpecification(0, Some(future))
    sendRequest(requestSpec, nodeSpec, retrySpec)
    future
  }

  @deprecated("Use sendRequest(PartitionedRequestSpecification[RequestMsg, PartitionedId], PartitionedNodeSpecification[PartitionedId], PartitionedRetrySpecification[ResponseMsg], responseAggregator), 2/2/2015")
  def sendRequest[RequestMsg, ResponseMsg](id: PartitionedId, request: RequestMsg, capability: Option[Long])
  (implicit is: InputSerializer[RequestMsg, ResponseMsg], os: OutputSerializer[RequestMsg, ResponseMsg]): Future[ResponseMsg] =
  {
    val future = new FutureAdapterListener[ResponseMsg]

    val requestSpec = PartitionedRequestSpecification[RequestMsg, PartitionedId](Some(request))
    val nodeSpec = new PartitionedNodeSpecification[PartitionedId](Set(id)).setCapability(capability).build
    val retrySpec = PartitionedRetrySpecification(0, Some(future))
    sendRequest(requestSpec, nodeSpec, retrySpec)
    future
  }

  @deprecated("Use sendRequest(PartitionedRequestSpecification[RequestMsg, PartitionedId], PartitionedNodeSpecification[PartitionedId], PartitionedRetrySpecification[ResponseMsg], responseAggregator), 2/2/2015")
  def sendRequest[RequestMsg, ResponseMsg](id: PartitionedId, request: RequestMsg, capability: Option[Long], persistentCapability: Option[Long])
  (implicit is: InputSerializer[RequestMsg, ResponseMsg], os: OutputSerializer[RequestMsg, ResponseMsg]): Future[ResponseMsg] = {
    val future = new FutureAdapterListener[ResponseMsg]

    val requestSpec = PartitionedRequestSpecification[RequestMsg, PartitionedId](Some(request))
    val nodeSpec = new PartitionedNodeSpecification[PartitionedId](Set(id)).setCapability(capability).setPersistentCapability(persistentCapability).build
    val retrySpec = PartitionedRetrySpecification(0, Some(future))
    sendRequest(requestSpec, nodeSpec, retrySpec)
    future
  }

  /**
   * Sends a <code>Message</code> to the specified <code>PartitionedId</code>s. The <code>PartitionedNetworkClient</code>
   * will interact with the current <code>PartitionedLoadBalancer</code> to calculate which <code>Node</code>s the message
   * must be sent to.  This method is asynchronous and will return immediately.
   *
   * @param ids the <code>PartitionedId</code>s to which the message is addressed
   * @param request the request to send
   *
   * @return a <code>ResponseIterator</code>. One response will be returned by each <code>Node</code>
   * the message was sent to.
   * @throws InvalidClusterException thrown if the cluster is currently in an invalid state
   * @throws NoNodesAvailableException thrown if the <code>PartitionedLoadBalancer</code> was unable to provide a <code>Node</code>
   * to send the request to
   * @throws ClusterDisconnectedException thrown if the <code>PartitionedNetworkClient</code> is not connected to the cluster
   */
  @deprecated("Use sendRequest(PartitionedRequestSpecification[RequestMsg, PartitionedId], PartitionedNodeSpecification[PartitionedId], PartitionedRetrySpecification[ResponseMsg], responseAggregator), 2/2/2015")
  def sendRequest[RequestMsg, ResponseMsg](ids: Set[PartitionedId], request: RequestMsg)
  (implicit is: InputSerializer[RequestMsg, ResponseMsg], os: OutputSerializer[RequestMsg, ResponseMsg]): ResponseIterator[ResponseMsg] =
  {
    val requestSpec = PartitionedRequestSpecification[RequestMsg, PartitionedId](Some(request))
    val nodeSpec = new PartitionedNodeSpecification[PartitionedId](ids).build
    val retrySpec = PartitionedRetrySpecification[ResponseMsg]()
    sendRequest(requestSpec, nodeSpec, retrySpec)(is, os)
  }

  @deprecated("Use sendRequest(PartitionedRequestSpecification[RequestMsg, PartitionedId], PartitionedNodeSpecification[PartitionedId], PartitionedRetrySpecification[ResponseMsg], responseAggregator), 2/2/2015")
  def sendRequest[RequestMsg, ResponseMsg](ids: Set[PartitionedId], request: RequestMsg, capability: Option[Long])
  (implicit is: InputSerializer[RequestMsg, ResponseMsg], os: OutputSerializer[RequestMsg, ResponseMsg]): ResponseIterator[ResponseMsg] =
  {
    val requestSpec = PartitionedRequestSpecification[RequestMsg, PartitionedId](Some(request))
    val nodeSpec = new PartitionedNodeSpecification[PartitionedId](ids).setCapability(capability).build
    val retrySpec = PartitionedRetrySpecification[ResponseMsg]()
    sendRequest(requestSpec, nodeSpec, retrySpec)
  }

  @deprecated("Use sendRequest(PartitionedRequestSpecification[RequestMsg, PartitionedId], PartitionedNodeSpecification[PartitionedId], PartitionedRetrySpecification[ResponseMsg], responseAggregator), 2/2/2015")
  def sendRequest[RequestMsg, ResponseMsg](ids: Set[PartitionedId], request: RequestMsg, capability: Option[Long], persistentCapability: Option[Long])
  (implicit is: InputSerializer[RequestMsg, ResponseMsg], os: OutputSerializer[RequestMsg, ResponseMsg]): ResponseIterator[ResponseMsg] =
  {
    val requestSpec = PartitionedRequestSpecification[RequestMsg, PartitionedId](Some(request))
    val nodeSpec = new PartitionedNodeSpecification[PartitionedId](ids).setCapability(capability).setPersistentCapability(persistentCapability).build
    val retrySpec = PartitionedRetrySpecification[ResponseMsg]()
    sendRequest(requestSpec, nodeSpec, retrySpec)
  }

  /**
   * Sends a <code>Message</code> to the specified <code>PartitionedId</code>s. The <code>PartitionedNetworkClient</code>
   * will interact with the current <code>PartitionedLoadBalancer</code> to calculate which <code>Node</code>s the message
   * must be sent to.  This method is asynchronous and will return immediately.
   *
   * @param ids the <code>PartitionedId</code>s to which the message is addressed
   * @param requestBuilder A method which allows the user to generate a specialized request for a set of partitions
   * before it is sent to the <code>Node</code>.
   *
   * @return a <code>ResponseIterator</code>. One response will be returned by each <code>Node</code>
   * the message was sent to.
   * @throws InvalidClusterException thrown if the cluster is currently in an invalid state
   * @throws NoNodesAvailableException thrown if the <code>PartitionedLoadBalancer</code> was unable to provide a <code>Node</code>
   * to send the request to
   * @throws ClusterDisconnectedException thrown if the <code>PartitionedNetworkClient</code> is not connected to the cluster
   */
  @deprecated("Use sendRequest(PartitionedRequestSpecification[RequestMsg, PartitionedId], PartitionedNodeSpecification[PartitionedId], PartitionedRetrySpecification[ResponseMsg], responseAggregator), 2/2/2015")
  def sendRequest[RequestMsg, ResponseMsg](ids: Set[PartitionedId], requestBuilder: (Node, Set[PartitionedId]) => RequestMsg)
  (implicit is: InputSerializer[RequestMsg, ResponseMsg], os: OutputSerializer[RequestMsg, ResponseMsg]): ResponseIterator[ResponseMsg] =
  {
    val requestSpec = PartitionedRequestSpecification[RequestMsg, PartitionedId](requestBuilder = Some(requestBuilder))
    val nodeSpec = new PartitionedNodeSpecification[PartitionedId](ids).build
    val retrySpec = PartitionedRetrySpecification[ResponseMsg](retryStrategy = retryStrategy)
    sendRequest(requestSpec, nodeSpec, retrySpec)
  }

  @deprecated("Use sendRequest(PartitionedRequestSpecification[RequestMsg, PartitionedId], PartitionedNodeSpecification[PartitionedId], PartitionedRetrySpecification[ResponseMsg], responseAggregator), 2/2/2015")
  def sendRequest[RequestMsg, ResponseMsg](ids: Set[PartitionedId], numberOfReplicas: Int, requestBuilder: (Node, Set[PartitionedId]) => RequestMsg)
                                          (implicit is: InputSerializer[RequestMsg, ResponseMsg], os: OutputSerializer[RequestMsg, ResponseMsg]): ResponseIterator[ResponseMsg] =
  {
    val requestSpec = PartitionedRequestSpecification[RequestMsg, PartitionedId](requestBuilder = Some(requestBuilder))
    val nodeSpec = new PartitionedNodeSpecification[PartitionedId](ids).setNumberOfReplicas(numberOfReplicas).build
    val retrySpec = PartitionedRetrySpecification[ResponseMsg](retryStrategy = retryStrategy)
    sendRequest(requestSpec, nodeSpec, retrySpec)
  }

  @deprecated("Use sendRequest(PartitionedRequestSpecification[RequestMsg, PartitionedId], PartitionedNodeSpecification[PartitionedId], PartitionedRetrySpecification[ResponseMsg], responseAggregator), 2/2/2015")
  def sendRequest[RequestMsg, ResponseMsg](ids: Set[PartitionedId], numberOfReplicas: Int, requestBuilder: (Node, Set[PartitionedId]) => RequestMsg, capability: Option[Long])
                                          (implicit is: InputSerializer[RequestMsg, ResponseMsg], os: OutputSerializer[RequestMsg, ResponseMsg]): ResponseIterator[ResponseMsg] =
  {
    val requestSpec = PartitionedRequestSpecification[RequestMsg, PartitionedId](requestBuilder = Some(requestBuilder))
    val nodeSpec = new PartitionedNodeSpecification[PartitionedId](ids).setCapability(capability).setNumberOfReplicas(numberOfReplicas).build
    val retrySpec = PartitionedRetrySpecification[ResponseMsg](retryStrategy = retryStrategy)
    sendRequest(requestSpec, nodeSpec, retrySpec)
  }

  @deprecated("Use sendRequest(PartitionedRequestSpecification[RequestMsg, PartitionedId], PartitionedNodeSpecification[PartitionedId], PartitionedRetrySpecification[ResponseMsg], responseAggregator), 2/2/2015")
  def sendRequest[RequestMsg, ResponseMsg](ids: Set[PartitionedId], requestBuilder: (Node, Set[PartitionedId]) => RequestMsg, capability: Option[Long])
  (implicit is: InputSerializer[RequestMsg, ResponseMsg], os: OutputSerializer[RequestMsg, ResponseMsg]): ResponseIterator[ResponseMsg] =
  {
    val requestSpec = PartitionedRequestSpecification[RequestMsg, PartitionedId](requestBuilder = Some(requestBuilder))
    val nodeSpec = new PartitionedNodeSpecification[PartitionedId](ids).setCapability(capability).build
    val retrySpec = PartitionedRetrySpecification[ResponseMsg](retryStrategy = retryStrategy)
    sendRequest(requestSpec, nodeSpec, retrySpec)
  }

  @deprecated("Use sendRequest(PartitionedRequestSpecification[RequestMsg, PartitionedId], PartitionedNodeSpecification[PartitionedId], PartitionedRetrySpecification[ResponseMsg], responseAggregator), 2/2/2015")
  def sendRequest[RequestMsg, ResponseMsg](ids: Set[PartitionedId], numberOfReplicas: Int, requestBuilder: (Node, Set[PartitionedId]) => RequestMsg, capability: Option[Long], dupOk : Boolean)
                                          (implicit is: InputSerializer[RequestMsg, ResponseMsg], os: OutputSerializer[RequestMsg, ResponseMsg]): ResponseIterator[ResponseMsg] =
  {
    val routingConfigs = new RoutingConfigs(retryStrategy != None, dupOk)
    val requestSpec = PartitionedRequestSpecification[RequestMsg, PartitionedId](requestBuilder = Some(requestBuilder))
    val nodeSpec = new PartitionedNodeSpecification[PartitionedId](ids).setCapability(capability).setNumberOfReplicas(numberOfReplicas).build
    val retrySpec = PartitionedRetrySpecification[ResponseMsg](retryStrategy = retryStrategy, routingConfigs = routingConfigs)
    sendRequest(requestSpec, nodeSpec, retrySpec)
  }

  @deprecated("Use sendRequest(PartitionedRequestSpecification[RequestMsg, PartitionedId], PartitionedNodeSpecification[PartitionedId], PartitionedRetrySpecification[ResponseMsg], responseAggregator), 2/2/2015")
  def sendRequest[RequestMsg, ResponseMsg](ids: Set[PartitionedId], requestBuilder: (Node, Set[PartitionedId]) => RequestMsg, capability: Option[Long], dupOk : Boolean)
                                          (implicit is: InputSerializer[RequestMsg, ResponseMsg], os: OutputSerializer[RequestMsg, ResponseMsg]): ResponseIterator[ResponseMsg] =
  {
    val routingConfigs = new RoutingConfigs(retryStrategy != None, dupOk)
    val requestSpec = PartitionedRequestSpecification[RequestMsg, PartitionedId](requestBuilder = Some(requestBuilder))
    val nodeSpec = new PartitionedNodeSpecification[PartitionedId](ids).setCapability(capability).build
    val retrySpec = PartitionedRetrySpecification[ResponseMsg](retryStrategy = retryStrategy, routingConfigs = routingConfigs)
    sendRequest(requestSpec, nodeSpec, retrySpec)
  }

  @deprecated("Use sendRequest(PartitionedRequestSpecification[RequestMsg, PartitionedId], PartitionedNodeSpecification[PartitionedId], PartitionedRetrySpecification[ResponseMsg], responseAggregator), 2/2/2015")
  def sendRequest[RequestMsg, ResponseMsg](ids: Set[PartitionedId], requestBuilder: (Node, Set[PartitionedId]) => RequestMsg, capability: Option[Long], routingConfigs : RoutingConfigs)
  (implicit is: InputSerializer[RequestMsg, ResponseMsg], os: OutputSerializer[RequestMsg, ResponseMsg]): ResponseIterator[ResponseMsg] =
  {
    val requestSpec = PartitionedRequestSpecification[RequestMsg, PartitionedId](requestBuilder = Some(requestBuilder))
    val nodeSpec = new PartitionedNodeSpecification[PartitionedId](ids).setCapability(capability).build
    val retrySpec = PartitionedRetrySpecification[ResponseMsg](retryStrategy = retryStrategy, routingConfigs = routingConfigs)
    sendRequest(requestSpec, nodeSpec, retrySpec)
  }

  @deprecated("Use sendRequest(PartitionedRequestSpecification[RequestMsg, PartitionedId], PartitionedNodeSpecification[PartitionedId], PartitionedRetrySpecification[ResponseMsg], responseAggregator), 2/2/2015")
  def sendRequest[RequestMsg, ResponseMsg](ids: Set[PartitionedId], requestBuilder: (Node, Set[PartitionedId]) => RequestMsg, capability: Option[Long], routingConfigs : RoutingConfigs, retryStrategy: Option[RetryStrategy])
  (implicit is: InputSerializer[RequestMsg, ResponseMsg], os: OutputSerializer[RequestMsg, ResponseMsg]): ResponseIterator[ResponseMsg] =
  {
    val requestSpec = PartitionedRequestSpecification[RequestMsg, PartitionedId](requestBuilder = Some(requestBuilder))
    val nodeSpec = new PartitionedNodeSpecification[PartitionedId](ids).setCapability(capability).build
    val retrySpec = PartitionedRetrySpecification[ResponseMsg](retryStrategy = retryStrategy, routingConfigs = routingConfigs)
    sendRequest(requestSpec, nodeSpec, retrySpec)
  }

  @deprecated("Use sendRequest(PartitionedRequestSpecification[RequestMsg, PartitionedId], PartitionedNodeSpecification[PartitionedId], PartitionedRetrySpecification[ResponseMsg], responseAggregator), 2/2/2015")
  def sendRequest[RequestMsg, ResponseMsg](ids: Set[PartitionedId], requestBuilder: (Node, Set[PartitionedId]) => RequestMsg, capability: Option[Long], persistentCapability: Option[Long])
  (implicit is: InputSerializer[RequestMsg, ResponseMsg], os: OutputSerializer[RequestMsg, ResponseMsg]): ResponseIterator[ResponseMsg] = doIfConnected {
    val requestSpec = PartitionedRequestSpecification[RequestMsg, PartitionedId](requestBuilder = Some(requestBuilder))
    val nodeSpec = new PartitionedNodeSpecification[PartitionedId](ids).setCapability(capability).setPersistentCapability(persistentCapability).build
    val retrySpec = PartitionedRetrySpecification[ResponseMsg](retryStrategy = retryStrategy)
    sendRequest(requestSpec, nodeSpec, retrySpec)
  }

  @deprecated("Use sendRequest(PartitionedRequestSpecification[RequestMsg, PartitionedId], PartitionedNodeSpecification[PartitionedId], PartitionedRetrySpecification[ResponseMsg], responseAggregator), 2/2/2015")
  def sendRequest[RequestMsg, ResponseMsg](ids: Set[PartitionedId], numberOfReplicas: Int, requestBuilder: (Node, Set[PartitionedId]) => RequestMsg, capability: Option[Long], persistentCapability: Option[Long])
                                          (implicit is: InputSerializer[RequestMsg, ResponseMsg], os: OutputSerializer[RequestMsg, ResponseMsg]): ResponseIterator[ResponseMsg] = doIfConnected {
    val requestSpec = PartitionedRequestSpecification[RequestMsg, PartitionedId](requestBuilder = Some(requestBuilder))
    val nodeSpec = new PartitionedNodeSpecification[PartitionedId](ids).setCapability(capability).setPersistentCapability(persistentCapability).setNumberOfReplicas(numberOfReplicas).build
    val retrySpec = PartitionedRetrySpecification[ResponseMsg](retryStrategy = retryStrategy)
    sendRequest(requestSpec, nodeSpec, retrySpec)
  }

  /**
   * Sends a <code>Message</code> to the specified <code>PartitionedId</code>s. The <code>PartitionedNetworkClient</code>
   * will interact with the current <code>PartitionedLoadBalancer</code> to calculate which <code>Node</code>s the message
   * must be sent to.  This method is asynchronous and will return immediately.
   *
   * @param ids the <code>PartitionedId</code>s to which the message is addressed
   * @param numberOfReplicas the number of replica you want to fan-out requests.
   * @param requestBuilder A method which allows the user to generate a specialized request for a set of partitions
   * before it is sent to the <code>Node</code>.
   * @param maxRetry maxium # of retry attempts
   *
   * @return a <code>ResponseIterator</code>. One response will be returned by each <code>Node</code>
   * the message was sent to.
   * @throws InvalidClusterException thrown if the cluster is currently in an invalid state
   * @throws NoNodesAvailableException thrown if the <code>PartitionedLoadBalancer</code> was unable to provide a <code>Node</code>
   * to send the request to
   * @throws ClusterDisconnectedException thrown if the <code>PartitionedNetworkClient</code> is not connected to the cluster
   */
  // TODO: investigate interplay between default parameter and implicits
  @deprecated("Use sendRequest(PartitionedRequestSpecification[RequestMsg, PartitionedId], PartitionedNodeSpecification[PartitionedId], PartitionedRetrySpecification[ResponseMsg], responseAggregator), 2/2/2015")
  def sendRequest[RequestMsg, ResponseMsg](ids: Set[PartitionedId], numberOfReplicas: Int, requestBuilder: (Node, Set[PartitionedId]) => RequestMsg, maxRetry: Int)
                                          (implicit is: InputSerializer[RequestMsg, ResponseMsg], os: OutputSerializer[RequestMsg, ResponseMsg]): ResponseIterator[ResponseMsg] =
  {
    val requestSpec = PartitionedRequestSpecification[RequestMsg, PartitionedId](requestBuilder = Some(requestBuilder))
    val nodeSpec = new PartitionedNodeSpecification[PartitionedId](ids).setNumberOfReplicas(numberOfReplicas).build
    val retrySpec = PartitionedRetrySpecification[ResponseMsg](maxRetry, retryStrategy = retryStrategy)
    sendRequest(requestSpec, nodeSpec, retrySpec)
  }

  @deprecated("Use sendRequest(PartitionedRequestSpecification[RequestMsg, PartitionedId], PartitionedNodeSpecification[PartitionedId], PartitionedRetrySpecification[ResponseMsg], responseAggregator), 2/2/2015")
  def sendRequest[RequestMsg, ResponseMsg](ids: Set[PartitionedId], requestBuilder: (Node, Set[PartitionedId]) => RequestMsg, maxRetry: Int)
  (implicit is: InputSerializer[RequestMsg, ResponseMsg], os: OutputSerializer[RequestMsg, ResponseMsg]): ResponseIterator[ResponseMsg] =
  {
    val requestSpec = PartitionedRequestSpecification[RequestMsg, PartitionedId](requestBuilder = Some(requestBuilder))
    val nodeSpec = new PartitionedNodeSpecification[PartitionedId](ids).build
    val retrySpec = PartitionedRetrySpecification[ResponseMsg](maxRetry, retryStrategy = retryStrategy)
    sendRequest(requestSpec, nodeSpec, retrySpec)
  }

  @deprecated("Use sendRequest(PartitionedRequestSpecification[RequestMsg, PartitionedId], PartitionedNodeSpecification[PartitionedId], PartitionedRetrySpecification[ResponseMsg], responseAggregator), 2/2/2015")
  def sendRequest[RequestMsg, ResponseMsg](ids: Set[PartitionedId], requestBuilder: (Node, Set[PartitionedId]) => RequestMsg, maxRetry: Int, capability: Option[Long])
  (implicit is: InputSerializer[RequestMsg, ResponseMsg], os: OutputSerializer[RequestMsg, ResponseMsg]): ResponseIterator[ResponseMsg] =
  {
    val requestSpec = PartitionedRequestSpecification[RequestMsg, PartitionedId](requestBuilder = Some(requestBuilder))
    val nodeSpec = new PartitionedNodeSpecification[PartitionedId](ids).setCapability(capability).build
    val retrySpec = PartitionedRetrySpecification[ResponseMsg](maxRetry, retryStrategy = retryStrategy)
    sendRequest(requestSpec, nodeSpec, retrySpec)
  }

  @deprecated("Use sendRequest(PartitionedRequestSpecification[RequestMsg, PartitionedId], PartitionedNodeSpecification[PartitionedId], PartitionedRetrySpecification[ResponseMsg]), 2/2/2015")
  def sendRequest[RequestMsg, ResponseMsg](ids: Set[PartitionedId], numberOfReplicas: Int,
      requestBuilder: (Node, Set[PartitionedId]) => RequestMsg, maxRetry: Int, capability: Option[Long],
      persistentCapability: Option[Long],
      routingConfigs: RoutingConfigs = new RoutingConfigs(retryStrategy != None, duplicatesOk),
      retryStrategy: Option[RetryStrategy] = retryStrategy)
    (implicit is: InputSerializer[RequestMsg, ResponseMsg],
        os: OutputSerializer[RequestMsg, ResponseMsg]): ResponseIterator[ResponseMsg] =
  {
    val requestSpec = PartitionedRequestSpecification[RequestMsg, PartitionedId](requestBuilder = Some(requestBuilder))
    val nodeSpec = new PartitionedNodeSpecification[PartitionedId](ids).setCapability(capability).setPersistentCapability(persistentCapability).setNumberOfReplicas(numberOfReplicas).build
    val retrySpec = PartitionedRetrySpecification[ResponseMsg](maxRetry, retryStrategy = retryStrategy, routingConfigs = routingConfigs)
    sendRequest(requestSpec, nodeSpec, retrySpec)
  }

  @deprecated("Use sendRequest(PartitionedRequestSpecification[RequestMsg, PartitionedId], PartitionedNodeSpecification[PartitionedId], PartitionedRetrySpecification[ResponseMsg]), 2/2/2015")
  def sendRequest[RequestMsg, ResponseMsg](ids: Set[PartitionedId], numberOfReplicas: Int, clusterId: Option[Int],
      requestBuilder: (Node, Set[PartitionedId]) => RequestMsg, maxRetry: Int, capability: Option[Long],
      persistentCapability: Option[Long],
      routingConfigs: RoutingConfigs,
      retryStrategy: Option[RetryStrategy])
    (implicit is: InputSerializer[RequestMsg, ResponseMsg],
        os: OutputSerializer[RequestMsg, ResponseMsg]): ResponseIterator[ResponseMsg] = doIfConnected
  {
    val requestSpec = PartitionedRequestSpecification[RequestMsg, PartitionedId](requestBuilder = Some(requestBuilder))
    val nodeSpec = new PartitionedNodeSpecification[PartitionedId](ids).setCapability(capability).setPersistentCapability(persistentCapability).setClusterId(clusterId).setNumberOfReplicas(numberOfReplicas).build
    val retrySpec = PartitionedRetrySpecification[ResponseMsg](maxRetry, retryStrategy = retryStrategy, routingConfigs = routingConfigs)
    sendRequest(requestSpec, nodeSpec, retrySpec)
  }

  /**
   * Sends a <code>Message</code> to the specified <code>PartitionedId</code>s. The <code>PartitionedNetworkClient</code>
   * will interact with the current <code>PartitionedLoadBalancer</code> to calculate which <code>Node</code>s the message
   * must be sent to.  This method is synchronous and will return once the responseAggregator has returned a value.
   *
   * @param ids the <code>PartitionedId</code>s to which the message is addressed
   * @param requestBuilder A method which allows the user to generate a specialized request for a set of partitions
   * before it is sent to the <code>Node</code>.
   * @param responseAggregator a callback method which allows the user to aggregate all the responses
   * and return a single object to the caller.  The callback will receive the original message passed to
   * <code>sendRequest</code> and the <code>ResponseIterator</code> for the request.
   *
   * @return the return value of the <code>responseAggregator</code>
   * @throws InvalidClusterException thrown if the cluster is currently in an invalid state
   * @throws NoNodesAvailableException thrown if the <code>PartitionedLoadBalancer</code> was unable to provide a <code>Node</code>
   * to send the request to
   * @throws ClusterDisconnectedException thrown if the <code>PartitionedNetworkClient</code> is not connected to the cluster
   * @throws Exception any exception thrown by <code>responseAggregator</code> will be passed through to the client
   */
  @deprecated("Use sendRequest(PartitionedRequestSpecification[RequestMsg, PartitionedId], PartitionedNodeSpecification[PartitionedId], PartitionedRetrySpecification[ResponseMsg], responseAggregator), 2/2/2015")
  def sendRequest[RequestMsg, ResponseMsg, Result](ids: Set[PartitionedId],
                                                   requestBuilder: (Node, Set[PartitionedId]) => RequestMsg,
                                                   responseAggregator: (ResponseIterator[ResponseMsg]) => Result)
  (implicit is: InputSerializer[RequestMsg, ResponseMsg], os: OutputSerializer[RequestMsg, ResponseMsg]): Result =
  {
    val requestSpec = PartitionedRequestSpecification[RequestMsg, PartitionedId](requestBuilder = Some(requestBuilder))
    val nodeSpec = new PartitionedNodeSpecification[PartitionedId](ids).build
    val retrySpec = PartitionedRetrySpecification[ResponseMsg](retryStrategy = retryStrategy)

    sendRequest(requestSpec, nodeSpec, retrySpec, responseAggregator)
  }

  @deprecated("Use sendRequest(PartitionedRequestSpecification[RequestMsg, PartitionedId], PartitionedNodeSpecification[PartitionedId], PartitionedRetrySpecification[ResponseMsg], responseAggregator), 2/2/2015")
  def sendRequest[RequestMsg, ResponseMsg, Result](ids: Set[PartitionedId],
                                                   numberOfReplicas: Int,
                                                   requestBuilder: (Node, Set[PartitionedId]) => RequestMsg,
                                                   responseAggregator: (ResponseIterator[ResponseMsg]) => Result)
                                                  (implicit is: InputSerializer[RequestMsg, ResponseMsg], os: OutputSerializer[RequestMsg, ResponseMsg]): Result =
  {
    val requestSpec = PartitionedRequestSpecification[RequestMsg, PartitionedId](requestBuilder = Some(requestBuilder))
    val nodeSpec = new PartitionedNodeSpecification[PartitionedId](ids).setNumberOfReplicas(numberOfReplicas).build
    val retrySpec = PartitionedRetrySpecification[ResponseMsg](retryStrategy = retryStrategy)

    sendRequest(requestSpec, nodeSpec, retrySpec, responseAggregator)
  }

  @deprecated("Use sendRequest(PartitionedRequestSpecification[RequestMsg, PartitionedId], PartitionedNodeSpecification[PartitionedId], PartitionedRetrySpecification[ResponseMsg], responseAggregator), 2/2/2015")
  def sendRequest[RequestMsg, ResponseMsg, Result](ids: Set[PartitionedId],
                                                   requestBuilder: (Node, Set[PartitionedId]) => RequestMsg,
                                                   responseAggregator: (ResponseIterator[ResponseMsg]) => Result,
                                                   capability: Option[Long])
  (implicit is: InputSerializer[RequestMsg, ResponseMsg], os: OutputSerializer[RequestMsg, ResponseMsg]): Result =
  {
    val requestSpec = PartitionedRequestSpecification[RequestMsg, PartitionedId](requestBuilder = Some(requestBuilder))
    val nodeSpec = new PartitionedNodeSpecification[PartitionedId](ids).setCapability(capability).build
    val retrySpec = PartitionedRetrySpecification[ResponseMsg](retryStrategy = retryStrategy)

    sendRequest(requestSpec, nodeSpec, retrySpec, responseAggregator)
  }

  @deprecated("Use sendRequest(PartitionedRequestSpecification[RequestMsg, PartitionedId], PartitionedNodeSpecification[PartitionedId], PartitionedRetrySpecification[ResponseMsg], responseAggregator), 2/2/2015")
  def sendRequest[RequestMsg, ResponseMsg, Result](ids: Set[PartitionedId],
                                                   requestBuilder: (Node, Set[PartitionedId]) => RequestMsg,
                                                   responseAggregator: (ResponseIterator[ResponseMsg]) => Result,
                                                   capability: Option[Long],
                                                   persistentCapability: Option[Long])
                                                  (implicit is: InputSerializer[RequestMsg, ResponseMsg], os: OutputSerializer[RequestMsg, ResponseMsg]): Result =
  {
    val requestSpec = PartitionedRequestSpecification[RequestMsg, PartitionedId](requestBuilder = Some(requestBuilder))
    val nodeSpec = new PartitionedNodeSpecification[PartitionedId](ids).setCapability(capability).setPersistentCapability(persistentCapability).build
    val retrySpec = PartitionedRetrySpecification[ResponseMsg](retryStrategy = retryStrategy)

    sendRequest(requestSpec, nodeSpec, retrySpec, responseAggregator)
  }

  @deprecated("Use sendRequest(PartitionedRequestSpecification[RequestMsg, PartitionedId], PartitionedNodeSpecification[PartitionedId], PartitionedRetrySpecification[ResponseMsg], responseAggregator), 2/2/2015")
  def sendRequest[RequestMsg, ResponseMsg, Result](ids: Set[PartitionedId],
                                                   numberOfReplicas: Int,
                                                   requestBuilder: (Node, Set[PartitionedId]) => RequestMsg,
                                                   responseAggregator: (ResponseIterator[ResponseMsg]) => Result,
                                                   capability: Option[Long],
                                                   persistentCapability: Option[Long])
  (implicit is: InputSerializer[RequestMsg, ResponseMsg], os: OutputSerializer[RequestMsg, ResponseMsg]): Result = doIfConnected {

    val requestSpec = PartitionedRequestSpecification[RequestMsg, PartitionedId](requestBuilder = Some(requestBuilder))
    val nodeSpec = new PartitionedNodeSpecification[PartitionedId](ids).setCapability(capability).setPersistentCapability(persistentCapability).setNumberOfReplicas(numberOfReplicas).build
    val retrySpec = PartitionedRetrySpecification[ResponseMsg](retryStrategy = retryStrategy)

    sendRequest(requestSpec, nodeSpec, retrySpec, responseAggregator)
  }

  // A version of the new sendRequest API (see below), that also takes a responseAggregator, a callback method that
  // allows the user to aggregate all the responses and return a single object to the caller.
  def sendRequest[RequestMsg, ResponseMsg, Result](requestSpec: PartitionedRequestSpecification[RequestMsg, PartitionedId],
                                           nodeSpec: PartitionedNodeSpecification[PartitionedId],
                                           retrySpec: PartitionedRetrySpecification[ResponseMsg],
                                           responseAggregator: (ResponseIterator[ResponseMsg]) => Result )
     (implicit is: InputSerializer[RequestMsg, ResponseMsg], os: OutputSerializer[RequestMsg, ResponseMsg]): Result = doIfConnected {

    if (responseAggregator == null) throw new NullPointerException
    responseAggregator(sendRequest[RequestMsg, ResponseMsg](requestSpec, nodeSpec, retrySpec))
  }


  /**
   * Sends a <code>Request</code> according to the requestSpec, nodeSpec, and retrySpec object specifications.
   * The <code>PartitionedNetworkClient</code>
   * will interact with the current <code>PartitionedLoadBalancer</code> to calculate which <code>Node</code>s the message
   * must be sent to.  This method is synchronous and will return once the responseAggregator has returned a value.
   *
   * @param requestSpec Specifies the message to be sent and the requestBuilder, a method which allows the user to
   *                    generate a specialized request for a set of partitions before it is sent to the <code>Node</code>.
   * @param nodeSpec Specifies the numberOfReplicas and the clusterId of the recipient <code>Node</code>s.
   * @param retrySpec Specifies the maxRetry (max number of retry attempts), a callback method,
   *                  a retryStrategy to apply in case of timeout, and a routingConfigs.
   *
   * @return the return value of the <code>responseAggregator</code>
   * @throws InvalidClusterException thrown if the cluster is currently in an invalid state
   * @throws NoNodesAvailableException thrown if the <code>PartitionedLoadBalancer</code> was unable to provide a <code>Node</code>
   * to send the request to
   * @throws ClusterDisconnectedException thrown if the <code>PartitionedNetworkClient</code> is not connected to the cluster
   * @throws Exception any exception thrown by <code>responseAggregator</code> will be passed through to the client
   */
  def sendRequest[RequestMsg, ResponseMsg](requestSpec: JPartitionedRequestSpecification[RequestMsg, PartitionedId], nodeSpec: JPartitionedNodeSpecification[PartitionedId], retrySpec: JPartitionedRetrySpecification[ResponseMsg, Unit])
                                          (implicit is: InputSerializer[RequestMsg, ResponseMsg], os: OutputSerializer[RequestMsg, ResponseMsg]): ResponseIterator[ResponseMsg] = doIfConnected
  {
    val requestBuilder = requestSpec.getRequestBuilder().getOrElse(throw new Exception("Request spec automatically creates a builder - this shouldn't happen ever."))

    if (nodeSpec.getIds() == null || requestBuilder == null) throw new NullPointerException

    // Convert clusterId from java.lang.Integer to scala.Int
    val clusterId = Option(Int.unbox(nodeSpec.getClusterId()))

    // Convert capability and persistentCapability from java.lang.Long to scala.Long
    val capability = Option(Long.unbox(nodeSpec.getCapability()))
    val persistentCapability = Option(Long.unbox(nodeSpec.getPersistentCapability()))

    val nodes = clusterId match {
      case Some(clusterId:Int) => calculateNodesFromIdsInCluster (nodeSpec.getIds(), clusterId, capability, persistentCapability)
      case None => calculateNodesFromIds (nodeSpec.getIds(), nodeSpec.getNumberOfReplicas(), capability, persistentCapability)
    }

    log.debug("Total number of ids: %d, selected nodes: %d, ids per node: [%s]".format(nodeSpec.getIds().size, nodes.size,
      nodes.view.map {
        case (node, idsForNode) => idsForNode.size
      } mkString("", ",", "")
    ))

    if (nodes.size <= 1 || !retrySpec.getRoutingConfigs().selectiveRetry || retrySpec.getRetryStrategy() == None) {
      val queue = new ResponseQueue[ResponseMsg]
      val resIter = new NorbertDynamicResponseIterator[ResponseMsg](nodes.size, queue)
      nodes.foreach { case (node, idsForNode) =>
        try {
        doSendRequest(PartitionedRequest(requestBuilder(node, idsForNode), node, idsForNode, requestBuilder, is, os, if (retrySpec.getMaxRetry() == 0) Some((a: Either[Throwable, ResponseMsg]) => {queue += a :Unit}) else Some(retryCallback[RequestMsg, ResponseMsg](queue.+=, retrySpec.getMaxRetry(), capability, persistentCapability)_), 0, Some(resIter)))
        } catch {
          case ex: Exception => queue += Left(ex)
        }
      }
      return resIter
    } else {
      val nodes = clusterId match {
        case Some(clusterId:Int) => calculateNodesFromIdsInCluster (nodeSpec.getIds(), clusterId, None, None)
        case None => calculateNodesFromIds (nodeSpec.getIds(), nodeSpec.getNumberOfReplicas(), None, None)
      }
      var setRequests: Map[PartitionedId, Node] = Map.empty[PartitionedId, Node]
      nodes.foreach {
        case (node, pids) => {
          pids.foreach{
            case(pid) => setRequests += pid->node
          }
        }
      }
      val queue = new ResponseQueue[Tuple3[Node, Set[PartitionedId], ResponseMsg]]

      /* wrapper so that iterator does not have to care about capability stuff */
      def calculateNodesFromIdsSRetry(ids: Set[PartitionedId], excludedNodes: Set[Node], maxAttempts: Int)
      :Map[Node, Set[PartitionedId]] = {
        calculateNodesFromIds(ids, excludedNodes, maxAttempts, capability, persistentCapability).toMap
      }

      val resIter = new SelectiveRetryIterator[PartitionedId, RequestMsg, ResponseMsg](
        nodes.size, retrySpec.getRetryStrategy().get.initialTimeout, doSendRequest, setRequests,
        queue, calculateNodesFromIdsSRetry, requestBuilder, is, os, retrySpec.getRetryStrategy(),
        retrySpec.getRoutingConfigs().duplicatesOk)

      nodes.foreach {
        case (node, idsForNode) => {
          def callback(a:Either[Throwable, ResponseMsg]):Unit = {
            a match {
              case Left(t) => queue += Left(t)
              case Right(r) => queue += Right(Tuple3(node, idsForNode, r))
            }
          }
          try {
            doSendRequest(PartitionedRequest(
              requestBuilder(node, idsForNode), node, idsForNode, requestBuilder, is, os,
              Some((a: Either[Throwable, ResponseMsg]) => {callback(a)}), 0, Some(resIter))
            )
          } catch {
            case ex: Exception => queue += Left(ex)
          }
        }
      }
      resIter
    }
  }

  /**
   * Sends a <code>RequestMessage</code> to one replica of the cluster. This is a broadcast intended for read operations on the cluster, like searching every partition for some data.
   *
   * @param id A partitioned id that can be used for consistent hashing purposes to ensure requests with the same id hit the same nodes in normal circumstances
   * @param request the request message to be sent
   *
   * @return a <code>ResponseIterator</code>. One response will be returned by each <code>Node</code>
   * the message was sent to.
   * @throws InvalidClusterException thrown if the cluster is currently in an invalid state
   * @throws NoNodesAvailableException thrown if the <code>PartitionedLoadBalancer</code> was unable to provide a <code>Node</code>
   * to send the request to
   * @throws ClusterDisconnectedException thrown if the <code>PartitionedNetworkClient</code> is not connected to the cluster
   */
  def sendRequestToOneReplica[RequestMsg, ResponseMsg](id: PartitionedId, request: RequestMsg)
  (implicit is: InputSerializer[RequestMsg, ResponseMsg], os: OutputSerializer[RequestMsg, ResponseMsg]): ResponseIterator[ResponseMsg] =
    sendRequestToOneReplica(id, request, None, None)

  def sendRequestToOneReplica[RequestMsg, ResponseMsg](id: PartitionedId, request: RequestMsg, capability: Option[Long])
  (implicit is: InputSerializer[RequestMsg, ResponseMsg], os: OutputSerializer[RequestMsg, ResponseMsg]): ResponseIterator[ResponseMsg] =
    sendRequestToOneReplica(id, request, capability, None)

  def sendRequestToOneReplica[RequestMsg, ResponseMsg](id: PartitionedId, request: RequestMsg, capability: Option[Long], persistentCapability: Option[Long])
  (implicit is: InputSerializer[RequestMsg, ResponseMsg], os: OutputSerializer[RequestMsg, ResponseMsg]): ResponseIterator[ResponseMsg]  =
    sendRequestToOneReplica(id, (node: Node, partitions: Set[Int]) => request, capability, persistentCapability)(is, os)


  /**
   * Sends a <code>RequestMessage</code> to one replica of the cluster. This is a broadcast intended for read operations on the cluster, like searching every partition for some data.
   * @param id A partitioned id that can be used for consistent hashing purposes to ensure requests with the same id hit the same nodes in normal circumstances
   * @param requestBuilder A function to generate a request for the chosen node/partitions to send the request to
   *
   * @return a <code>ResponseIterator</code>. One response will be returned by each <code>Node</code>
   * the message was sent to.
   * @throws InvalidClusterException thrown if the cluster is currently in an invalid state
   * @throws NoNodesAvailableException thrown if the <code>PartitionedLoadBalancer</code> was unable to provide a <code>Node</code>
   * to send the request to
   * @throws ClusterDisconnectedException thrown if the <code>PartitionedNetworkClient</code> is not connected to the cluster
   */
  def sendRequestToOneReplica[RequestMsg, ResponseMsg](id: PartitionedId, requestBuilder: (Node, Set[Int]) => RequestMsg)
                                                      (implicit is: InputSerializer[RequestMsg, ResponseMsg],
                                                       os: OutputSerializer[RequestMsg, ResponseMsg]): ResponseIterator[ResponseMsg]  =
    sendRequestToOneReplica(id, requestBuilder, None, None)

  def sendRequestToOneReplica[RequestMsg, ResponseMsg](id: PartitionedId, requestBuilder: (Node, Set[Int]) => RequestMsg, capability: Option[Long])
                                                      (implicit is: InputSerializer[RequestMsg, ResponseMsg],
                                                       os: OutputSerializer[RequestMsg, ResponseMsg]): ResponseIterator[ResponseMsg] =
    sendRequestToOneReplica(id, requestBuilder, capability, None)

  def sendRequestToOneReplica[RequestMsg, ResponseMsg](id: PartitionedId, requestBuilder: (Node, Set[Int]) => RequestMsg, capability: Option[Long], persistentCapability: Option[Long])
                                                      (implicit is: InputSerializer[RequestMsg, ResponseMsg],
                                                       os: OutputSerializer[RequestMsg, ResponseMsg]): ResponseIterator[ResponseMsg]  = doIfConnected {
    val nodes = loadBalancer.getOrElse(throw new ClusterDisconnectedException).fold(ex => throw ex,
      lb => lb.nodesForOneReplica(id, capability, persistentCapability))

    if (nodes.isEmpty) throw new NoNodesAvailableException("Unable to satisfy request, no node available for request")

    val queue = new ResponseQueue[ResponseMsg]

    ensureReplicaConsistency(nodes).foreach { case (node, ids) =>
      doSendRequest(PartitionedRequest(requestBuilder(node, ids), node, ids, requestBuilder, is, os, Some((a:Either[Throwable, ResponseMsg]) => queue += a :Unit)))
    }

    new NorbertResponseIterator(nodes.size, queue)
  }

  /**
   * Catch & log inconsistencies in the request handling, and then correct them.
   * This shouldn't happen but we've gotten bug reports. I really hate doing this.
   * @param nodes
   */
  def ensureReplicaConsistency(nodes: Map[Node, Set[Int]]): Map[Node, Set[Int]] = {
    val (hasInconsistency, partitionToNodes) =
      nodes.foldLeft((false, Map.empty[Int, Set[Node]])) { case ((hasInconsistency, map), (node, ids)) =>

      val thisNodeInconsistency = ids.foldLeft(hasInconsistency) { (hasInconsistency, id) =>
        if(map.contains(id)) {
          // This is a no-no. This partition id is being sent to another node.
          val otherNodes = map(id)
          for (otherNode <- otherNodes) {
            val otherPartitions = nodes.getOrElse(otherNode, Set.empty[Int])

            log.warn("Request conflict found between [%s, Searching Partitions (%s)]; [%s, Searching Partitions (%s)]"
              .format(node, ids.mkString(", "), otherNode, otherPartitions.mkString(", ")))

          }
          true
        } else {
          hasInconsistency
        }
      }

      // Keep track of what partitions were assigned to which nodes
      (thisNodeInconsistency, ids.foldLeft(map) { case (map, id) =>
        val mapValue = map.getOrElse(id, Set.empty[Node])
        map + (id -> (mapValue + node))
      })
    }

    if(hasInconsistency) {
      // Fix it up our nodes
      correctRequestPartitioning(nodes, partitionToNodes)
    } else {
      // all clean
      nodes
    }
  }

  private val random = new Random

  def correctRequestPartitioning(nodes: Map[Node, Set[Int]], partitionToNodes: Map[Int, Set[Node]]): Map[Node, Set[Int]] = {
    partitionToNodes.foldLeft(Map.empty[Node, Set[Int]]) { case (map, (partitionId, candidates)) =>
      val nodeToUse = if (candidates.size == 1) {
        candidates.head
      } else {
        // randomly select
        val candidateSeq = candidates.toSeq
        val randomIndex = random.nextInt(candidateSeq.size)
        candidateSeq(randomIndex)
      }
    {

      val nodePartitions = map.getOrElse(nodeToUse, Set.empty[Int])
      map + (nodeToUse -> (nodePartitions + partitionId))
    }
    }
  }

  def sendRequestToReplicas[RequestMsg, ResponseMsg](id: PartitionedId, request: RequestMsg, maxRetry : Int = 0)
                                                    (implicit is: InputSerializer[RequestMsg, ResponseMsg], os: OutputSerializer[RequestMsg, ResponseMsg]): ResponseIterator[ResponseMsg] =
    sendRequestToReplicas(id, request, maxRetry, None, None)

  def sendRequestToReplicas[RequestMsg, ResponseMsg](id: PartitionedId, request: RequestMsg, maxRetry : Int, capability: Option[Long])
                                                      (implicit is: InputSerializer[RequestMsg, ResponseMsg], os: OutputSerializer[RequestMsg, ResponseMsg]): ResponseIterator[ResponseMsg] = sendRequestToReplicas(id, request, maxRetry, capability, None)

  def sendRequestToReplicas[RequestMsg, ResponseMsg](id: PartitionedId, request: RequestMsg, maxRetry : Int, capability: Option[Long], persistentCapability: Option[Long])
                                                       (implicit is: InputSerializer[RequestMsg, ResponseMsg], os: OutputSerializer[RequestMsg, ResponseMsg]): ResponseIterator[ResponseMsg] = doIfConnected {
    if (id == null || request == null) throw new NullPointerException
    val nodes = loadBalancer.getOrElse(throw new ClusterDisconnectedException).fold(ex => throw ex,
                                                                                    lb =>
                                                                                    {
                                                                                      val nodeSet = lb.nodesForPartitionedId(id, capability, persistentCapability)
                                                                                      if (nodeSet.isEmpty) throw new NoNodesAvailableException("Unable to satisfy request, no node available for id %s".format(id))
                                                                                      nodeSet
                                                                                    })
    val queue = new ResponseQueue[ResponseMsg]
    val resIter = new NorbertDynamicResponseIterator[ResponseMsg](nodes.size, queue)
    nodes.foreach { case (node) =>
      try {
        doSendRequest(PartitionedRequest(request, node, Set(id), (node: Node, ids: Set[PartitionedId]) => request, is, os, if (maxRetry == 0) Some((a:Either[Throwable, ResponseMsg]) => {queue += a: Unit}) else Some(retryCallback[RequestMsg, ResponseMsg](queue.+=, maxRetry, capability, persistentCapability) _), 0, Some(resIter)))
      } catch {
        case ex: Exception => queue += Left(ex)
      }
    }
    resIter
  }


  /**
   * Sends a <code>RequestMessage</code> to a set of partitions in the cluster. This is a broadcast intended for read operations on the cluster, like searching every partition for some data.
   *
   * @param id A partitioned id that can be used for consistent hashing purposes to ensure requests with the same id hit the same nodes in normal circumstances
   * @param requestBuilder A function to generate a request for the chosen node/partitions to send the request to
   *
   * @return a <code>ResponseIterator</code>. One response will be returned by each <code>Node</code>
   * the message was sent to.
   * @throws InvalidClusterException thrown if the cluster is currently in an invalid state
   * @throws NoNodesAvailableException thrown if the <code>PartitionedLoadBalancer</code> was unable to provide a <code>Node</code>
   * to send the request to
   * @throws ClusterDisconnectedException thrown if the <code>PartitionedNetworkClient</code> is not connected to the cluster
   */
  def sendRequestToPartitions[RequestMsg, ResponseMsg](id: PartitionedId, partitions: Set[Int], requestBuilder: (Node, Set[Int]) => RequestMsg)
                                                      (implicit is: InputSerializer[RequestMsg, ResponseMsg],
                                                       os: OutputSerializer[RequestMsg, ResponseMsg]): ResponseIterator[ResponseMsg]  =
    sendRequestToPartitions(id, partitions, requestBuilder, None, None)

  def sendRequestToPartitions[RequestMsg, ResponseMsg](id: PartitionedId, partitions: Set[Int], requestBuilder: (Node, Set[Int]) => RequestMsg, capability: Option[Long])
                                                      (implicit is: InputSerializer[RequestMsg, ResponseMsg],
                                                       os: OutputSerializer[RequestMsg, ResponseMsg]): ResponseIterator[ResponseMsg]  =
    sendRequestToPartitions(id, partitions, requestBuilder, None, None)

  def sendRequestToPartitions[RequestMsg, ResponseMsg](id: PartitionedId, partitions: Set[Int], requestBuilder: (Node, Set[Int]) => RequestMsg, capability: Option[Long], persistentCapability: Option[Long])
                                                      (implicit is: InputSerializer[RequestMsg, ResponseMsg],
                                                       os: OutputSerializer[RequestMsg, ResponseMsg]): ResponseIterator[ResponseMsg]  = doIfConnected {
    val nodes = loadBalancer.getOrElse(throw new ClusterDisconnectedException).fold(ex => throw ex,
      lb => lb.nodesForPartitions(id, partitions))

    if (nodes.isEmpty) throw new NoNodesAvailableException("Unable to satisfy request, no node available for request")

    val queue = new ResponseQueue[ResponseMsg]

    ensureReplicaConsistency(nodes).foreach { case (node, ids) =>
      doSendRequest(PartitionedRequest(requestBuilder(node, ids), node, ids, requestBuilder, is, os, Option((a:Either[Throwable, ResponseMsg]) => {queue += a :Unit})))
    }

    new NorbertResponseIterator(nodes.size, queue)
  }

  protected def updateLoadBalancer(endpoints: Set[Endpoint]) {
    loadBalancer = if (endpoints != null && endpoints.size > 0) {
      try {
        Some(Right(loadBalancerFactory.newLoadBalancer(endpoints)))
      } catch {
        case ex: InvalidClusterException =>
          log.info(ex, "Unable to create new router instance")
          Some(Left(ex))

        case ex: Exception =>
          val msg = "Exception while creating new router instance"
          log.error(ex, msg)
          Some(Left(new InvalidClusterException(msg, ex)))
      }
    } else {
      None
    }
  }

  /**
   * Internal callback wrapper to handle partial failures via RequestAccess
   */
  private[partitioned] def retryCallback[RequestMsg, ResponseMsg](underlying: Either[Throwable, ResponseMsg] => Unit, maxRetry: Int, capability: Option[Long], persistentCapability: Option[Long])(res: Either[Throwable, ResponseMsg])
  (implicit is: InputSerializer[RequestMsg, ResponseMsg], os: OutputSerializer[RequestMsg, ResponseMsg]): Unit = {
    def propagate(t: Throwable) { log.info("Propagate exception(%s) to client".format(t)); underlying(Left(t)) }
    def handleFailure(t: Throwable) {
      t match {
        case ra: RequestAccess[PartitionedRequest[PartitionedId, RequestMsg, ResponseMsg]] =>
          log.info("Caught exception(%s) for request %s".format(ra, ra.request))
          val prequest = ra.request
          val requestBuilder = prequest.requestBuilder
          if (prequest.retryAttempt < maxRetry && prequest.responseIterator.isDefined && prequest.responseIterator.get.isInstanceOf[DynamicResponseIterator[ResponseMsg]]) {
            try {
              val nodes = calculateNodesFromIds(prequest.partitionedIds, Set(prequest.node), 3, capability, persistentCapability)
              if (nodes.keySet.size > 1) {
                log.debug("Adjust responseIterator size by: %d".format(nodes.keySet.size - 1))
                prequest.responseIterator.get.asInstanceOf[DynamicResponseIterator[ResponseMsg]].addAndGet(nodes.keySet.size - 1)
              }
              nodes.foreach {
                case (node, idsForNode) =>
                  val request1 = PartitionedRequest(requestBuilder(node, idsForNode), node, idsForNode, requestBuilder, is, os, Some(retryCallback[RequestMsg, ResponseMsg](underlying, maxRetry, capability, persistentCapability) _), prequest.retryAttempt + 1, prequest.responseIterator)
                  log.debug("Resend request: %s".format(request1))
                  doSendRequest(request1)
              }
            } catch {
              case t1: Throwable =>
                log.debug("Exception(%s) caught during retry".format(t1))
                propagate(t)
            }
          } else propagate(t)
        case _: Throwable => propagate(t)
      }
    }
    if (underlying == null)
      throw new NullPointerException
    if (maxRetry <= 0)
      res.fold(t => propagate(t), result => underlying(Right(result)))
    else
      res.fold(t => handleFailure(t), result => underlying(Right(result)))
  }

  private def calculateNodesFromIds(ids: Set[PartitionedId], capability: Option[Long], persistentCapability: Option[Long]) = {
    val lb = loadBalancer.getOrElse(throw new ClusterDisconnectedException).fold(ex => throw ex, lb => lb)
    ids.foldLeft(Map[Node, Set[PartitionedId]]().withDefaultValue(Set())) { (map, id) =>
      val node = lb.nextNode(id, capability, persistentCapability).getOrElse(throw new NoNodesAvailableException("Unable to satisfy request, no node available for id %s".format(id)))
      map.updated(node, map(node) + id)
    }
  }

  private def calculateNodesFromIds(ids: Set[PartitionedId], numberOfReplicas: Int, capability: Option[Long],
                                    persistentCapability: Option[Long]): Map[Node,
    Set[PartitionedId]] = numberOfReplicas match {
    case 0 => calculateNodesFromIds(ids, capability, persistentCapability)
    case _ => {
      val lb = loadBalancer.getOrElse(throw new ClusterDisconnectedException).fold(ex => throw ex, lb => lb)
      lb.nodesForPartitionedIdsInNReplicas(ids, numberOfReplicas, capability, persistentCapability)
    }
  }

  private def calculateNodesFromIdsInCluster (ids: Set[PartitionedId], clusterId: Int, capability: Option[Long],
      persistentCapability: Option[Long]): Map[Node, Set[PartitionedId]] = {
    val lb = loadBalancer.getOrElse(throw new ClusterDisconnectedException).fold(ex => throw ex, lb => lb)
    lb.nodesForPartitionedIdsInOneCluster(ids, clusterId, capability, persistentCapability)
  }

  /**
   * For retry attempts. Failing nodes excluded
   */
  private[partitioned] def calculateNodesFromIds(ids: Set[PartitionedId], excludedNodes: Set[Node], maxAttempts: Int,capability: Option[Long], persistentCapability: Option[Long]) = {
    if (maxAttempts <= 0)
      throw new IllegalArgumentException
    val lb = loadBalancer.getOrElse(throw new ClusterDisconnectedException).fold(ex => throw ex, lb => lb)
    val map = collection.mutable.Map[Node, Set[PartitionedId]]()
    ids.foreach { id =>
      var foundIt = false
      var i = 0
      var node: Node = null
      while (i < maxAttempts && !foundIt) {
        node = lb.nextNode(id, capability, persistentCapability).getOrElse(throw new NoNodesAvailableException("Unable to satisfy request, no node available for id %s".format(id)))
        if (!excludedNodes.contains(node)) {
          foundIt = true
        }
        i += 1
      }
      if (foundIt) {
        if (map contains node) map.updated(node, map(node) + id) else map.put(node, Set(id))
      } else {
        throw new NoNodesAvailableException("Unable to satisfy request, no node available for id %s".format(id))
      }
    }
    map
  }

}

