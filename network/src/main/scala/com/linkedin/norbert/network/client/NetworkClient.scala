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
package client

import java.util.concurrent.Future
import loadbalancer.{LoadBalancerFactory, LoadBalancer, LoadBalancerFactoryComponent}
import server.{MessageExecutorComponent, NetworkServer}
import cluster._
import network.common._
import netty.NettyNetworkClient


object NetworkClientConfig {
  var defaultIteratorTimeout = NetworkDefaults.DEFAULT_ITERATOR_TIMEOUT;
}

class NetworkClientConfig {
  var clusterClient: ClusterClient = _
  var clientName: String = _
  var serviceName: String = _
  var zooKeeperConnectString: String = _
  var zooKeeperSessionTimeoutMillis = ClusterDefaults.ZOOKEEPER_SESSION_TIMEOUT_MILLIS

  var connectTimeoutMillis = NetworkDefaults.CONNECT_TIMEOUT_MILLIS
  var writeTimeoutMillis = NetworkDefaults.WRITE_TIMEOUT_MILLIS
  var maxConnectionsPerNode = NetworkDefaults.MAX_CONNECTIONS_PER_NODE

  var staleRequestTimeoutMins = NetworkDefaults.STALE_REQUEST_TIMEOUT_MINS
  var staleRequestCleanupFrequenceMins = NetworkDefaults.STALE_REQUEST_CLEANUP_FREQUENCY_MINS

  /**
   * Represents how long a channel stays alive. There are some specifics:
   * closeChannelTimeMillis < 0: Channel stays alive forever
   * closeChannelTimeMillis == 0: Immediately close the channel
   * closeChannelTimeMillis > 0: Close the channel after closeChannelTimeMillis
   */
  var closeChannelTimeMillis = NetworkDefaults.CLOSE_CHANNEL_TIMEOUT_MILLIS

  var requestStatisticsWindow = NetworkDefaults.REQUEST_STATISTICS_WINDOW

  var outlierMuliplier = NetworkDefaults.OUTLIER_MULTIPLIER
  var outlierConstant = NetworkDefaults.OUTLIER_CONSTANT

  var responseHandlerCorePoolSize = NetworkDefaults.RESPONSE_THREAD_CORE_POOL_SIZE
  var responseHandlerMaxPoolSize = NetworkDefaults.RESPONSE_THREAD_MAX_POOL_SIZE
  var responseHandlerKeepAliveTime = NetworkDefaults.RESPONSE_THREAD_KEEP_ALIVE_TIME_SECS
  var responseHandlerMaxWaitingQueueSize = NetworkDefaults.RESPONSE_THREAD_POOL_QUEUE_SIZE

  var avoidByteStringCopy = NetworkDefaults.AVOID_BYTESTRING_COPY
  var darkCanaryServiceName: Option[String] = None
  var retryStrategy:Option[RetryStrategy] = None 
  var duplicatesOk:Boolean = false
}

object NetworkClient {
  def apply(config: NetworkClientConfig, loadBalancerFactory: LoadBalancerFactory): NetworkClient = {
    val nc = new NettyNetworkClient(config, loadBalancerFactory)
    nc.start
    nc
  }

  def apply(config: NetworkClientConfig, loadBalancerFactory: LoadBalancerFactory, server: NetworkServer): NetworkClient = {
    val nc = new NettyNetworkClient(config, loadBalancerFactory) with LocalMessageExecution with MessageExecutorComponent {
      val messageExecutor = server.asInstanceOf[MessageExecutorComponent].messageExecutor
      val myNode = server.myNode
    }
    nc.start
    nc
  }
}


//TODO: mark all the old functions deprecated and reroute them to the new function definition
/**
 * The network client interface for interacting with nodes in a cluster.
 */
trait NetworkClient extends BaseNetworkClient {
  this: ClusterClientComponent with ClusterIoClientComponent with LoadBalancerFactoryComponent =>

  @volatile private var loadBalancer: Option[Either[InvalidClusterException, LoadBalancer]] = None

  /**
   * Sends a request to a node in the cluster. The <code>NetworkClient</code> defers to the current
   * <code>LoadBalancer</code> to decide which <code>Node</code> the request should be sent to.
   *
   * @param request the message to send
   * @param callback a method to be called with either a Throwable in the case of an error along
   * the way or a ResponseMsg representing the result
   *
   * @throws InvalidClusterException thrown if the cluster is currently in an invalid state
   * @throws NoNodesAvailableException thrown if the <code>LoadBalancer</code> was unable to provide a <code>Node</code>
   * to send the request to
   * @throws ClusterDisconnectedException thrown if the cluster is not connected when the method is called
   */
  def sendRequest[RequestMsg, ResponseMsg](request: RequestMsg, callback: Either[Throwable, ResponseMsg] => Unit)
  (implicit is: InputSerializer[RequestMsg, ResponseMsg], os: OutputSerializer[RequestMsg, ResponseMsg]): Unit =  
    sendRequest(request, callback, None, None)

  def sendRequest[RequestMsg, ResponseMsg](request: RequestMsg, callback: Either[Throwable, ResponseMsg] => Unit, capability: Option[Long])
  (implicit is: InputSerializer[RequestMsg, ResponseMsg], os: OutputSerializer[RequestMsg, ResponseMsg]): Unit = 
     sendRequest(request, callback, capability, None)

  def sendRequest[RequestMsg, ResponseMsg](request: RequestMsg, callback: Either[Throwable, ResponseMsg] => Unit, capability: Option[Long], persistentCapability: Option[Long])
  (implicit is: InputSerializer[RequestMsg, ResponseMsg], os: OutputSerializer[RequestMsg, ResponseMsg]): Unit =  doIfConnected {
     sendRequest(request, callback, 0, capability, persistentCapability)
  }
  
  /**
   * Sends a request to a node in the cluster. The <code>NetworkClient</code> defers to the current
   * <code>LoadBalancer</code> to decide which <code>Node</code> the request should be sent to.
   *
   * @param request the message to send
   *
   * @return a future which will become available when a response to the request is received
   * @throws InvalidClusterException thrown if the cluster is currently in an invalid state
   * @throws NoNodesAvailableException thrown if the <code>LoadBalancer</code> was unable to provide a <code>Node</code>
   * to send the request to
   * @throws ClusterDisconnectedException thrown if the cluster is not connected when the method is called
   */

  def sendRequest[RequestMsg, ResponseMsg](request: RequestMsg)
  (implicit is: InputSerializer[RequestMsg, ResponseMsg], os: OutputSerializer[RequestMsg, ResponseMsg]): Future[ResponseMsg] =
    sendRequest(request, None, None)

  def sendRequest[RequestMsg, ResponseMsg](request: RequestMsg, maxRetry:Int)
  (implicit is: InputSerializer[RequestMsg, ResponseMsg], os: OutputSerializer[RequestMsg, ResponseMsg]): Future[ResponseMsg] =
    sendRequest(request, maxRetry, None, None)

  def sendRequest[RequestMsg, ResponseMsg](request: RequestMsg, capability: Option[Long])
  (implicit is: InputSerializer[RequestMsg, ResponseMsg], os: OutputSerializer[RequestMsg, ResponseMsg]): Future[ResponseMsg] = {
    sendRequest(request, capability, None)
  }
  
  def sendRequest[RequestMsg, ResponseMsg](request: RequestMsg, capability: Option[Long], persistentCapability: Option[Long])
  (implicit is: InputSerializer[RequestMsg, ResponseMsg], os: OutputSerializer[RequestMsg, ResponseMsg]): Future[ResponseMsg] = {
    val future = new FutureAdapterListener[ResponseMsg]
    sendRequest(request, future, capability, persistentCapability)
    future
  }

  def sendRequest[RequestMsg, ResponseMsg](request: RequestMsg, maxRetry: Int, capability: Option[Long])
  (implicit is: InputSerializer[RequestMsg, ResponseMsg], os: OutputSerializer[RequestMsg, ResponseMsg]): Future[ResponseMsg] = {
    val future = new FutureAdapterListener[ResponseMsg]
    sendRequest(request, future, maxRetry, capability, None)
    future
  }

  def sendRequest[RequestMsg, ResponseMsg](request: RequestMsg, maxRetry: Int, capability: Option[Long], persistentCapability: Option[Long])
  (implicit is: InputSerializer[RequestMsg, ResponseMsg], os: OutputSerializer[RequestMsg, ResponseMsg]): Future[ResponseMsg] = {
    val future = new FutureAdapterListener[ResponseMsg]
    sendRequest(request, future, maxRetry, capability, persistentCapability)
    future
  }

  /**
   * Sends a request to a node in the cluster. The <code>NetworkClient</code> defers to the current
   * <code>LoadBalancer</code> to decide which <code>Node</code> the request should be sent to.
   *
   * @param request the message to send
   * @param callback a method to be called with either a Throwable in the case of an error along
   * the way or a ResponseMsg representing the result
   * @param maxRetry maximum # of retry attempts
   *
   * @throws InvalidClusterException thrown if the cluster is currently in an invalid state
   * @throws NoNodesAvailableException thrown if the <code>LoadBalancer</code> was unable to provide a <code>Node</code>
   * to send the request to
   * @throws ClusterDisconnectedException thrown if the cluster is not connected when the method is called
   */
  def sendRequest[RequestMsg, ResponseMsg](request: RequestMsg, callback: Either[Throwable, ResponseMsg] => Unit, maxRetry: Int)
  (implicit is: InputSerializer[RequestMsg, ResponseMsg], os: OutputSerializer[RequestMsg, ResponseMsg]): Unit =
    sendRequest(request, callback, maxRetry, None, None)

  def sendRequest[RequestMsg, ResponseMsg](request: RequestMsg, callback: Either[Throwable, ResponseMsg] => Unit, maxRetry: Int, capability: Option[Long])
  (implicit is: InputSerializer[RequestMsg, ResponseMsg], os:OutputSerializer[RequestMsg, ResponseMsg]): Unit = 
    sendRequest(request, callback, maxRetry, capability, None)

  def sendRequest[RequestMsg, ResponseMsg](request: RequestMsg, callback: Either[Throwable, ResponseMsg] => Unit, maxRetry: Int, capability: Option[Long], persistentCapability: Option[Long])
                                          (implicit is: InputSerializer[RequestMsg, ResponseMsg], os: OutputSerializer[RequestMsg, ResponseMsg]): Unit = doIfConnected {
    if (request == null) throw new NullPointerException

    val loadBalancerReady = loadBalancer.getOrElse(throw new ClusterDisconnectedException("Client has no node information"))

    val node = loadBalancerReady.fold(ex => throw ex,
      lb => {
        val node: Option[Node] = lb.nextNode(capability, persistentCapability)
        node.getOrElse(throw new NoNodesAvailableException("No node available that can handle the request: %s".format(request)))
      })

    doSendRequest(Request(request, node, is, os, if (maxRetry == 0) Some(callback) else Some(retryCallback[RequestMsg, ResponseMsg](callback, maxRetry, capability, persistentCapability)_)))
  }

  /**
   * TODO: comment new function
   */
  def sendRequest[RequestMsg, ResponseMsg](requestSpec: RequestSpecification[RequestMsg], nodeSpec: NodeSpec, retrySpec: RetrySpecifications[ResponseMsg])
  (implicit is: InputSerializer[RequestMsg, ResponseMsg], os:OutputSerializer[RequestMsg, ResponseMsg]) = doIfConnected {
    if (requestSpec.message == null) throw new NullPointerException
    val callback = retrySpec.callback.getOrElse(throw new Exception("No callback and no default callback"));

    val loadBalancerReady = loadBalancer.getOrElse(throw new ClusterDisconnectedException("Client has no node information"))

    val node = loadBalancerReady.fold(ex => throw ex,
      lb => {
        val node: Option[Node] = lb.nextNode(nodeSpec.capability, nodeSpec.persistentCapability)
        node.getOrElse(throw new NoNodesAvailableException("No node available that can handle the request: %s".format(requestSpec.message)))
      })

    doSendRequest(Request(requestSpec.message, node, is, os, if (retrySpec.maxRetry == 0) Some(callback) else Some(retryCallback[RequestMsg, ResponseMsg](callback, retrySpec.maxRetry, nodeSpec.capability, nodeSpec.persistentCapability) _)))
  }


  /**
   * Sends a one way message to a node in the cluster. The <code>NetworkClient</code> defers to the current
   * <code>LoadBalancer</code> to decide which <code>Node</code> the request should be sent to.
   *
   * @param request the message to send
   *
   * @throws InvalidClusterException thrown if the cluster is currently in an invalid state
   * @throws NoNodesAvailableException thrown if the <code>LoadBalancer</code> was unable to provide a <code>Node</code>
   * to send the request to
   * @throws ClusterDisconnectedException thrown if the cluster is not connected when the method is called
   */

  def sendMessage[RequestMsg, ResponseMsg](request: RequestMsg)
                                          (implicit is: InputSerializer[RequestMsg, ResponseMsg], os: OutputSerializer[RequestMsg, ResponseMsg]) {
    doIfConnected {
      sendMessage(request, None, None)
    }
  }

  def sendMessage[RequestMsg, ResponseMsg](request: RequestMsg, capability: Option[Long])
                                          (implicit is: InputSerializer[RequestMsg, ResponseMsg], os: OutputSerializer[RequestMsg, ResponseMsg]) {
    doIfConnected {
      sendMessage(request, capability, None)
    }
  }

  def sendMessage[RequestMsg, ResponseMsg](request: RequestMsg, capability: Option[Long], persistentCapability: Option[Long])
                                          (implicit is: InputSerializer[RequestMsg, ResponseMsg], os: OutputSerializer[RequestMsg, ResponseMsg]) {
    doIfConnected {
      if (request == null) throw new NullPointerException

      val loadBalancerReady = loadBalancer.getOrElse(throw new ClusterDisconnectedException("Client has no node information"))

      val node = loadBalancerReady.fold(ex => throw ex,
        lb => {
          val node: Option[Node] = lb.nextNode(capability, persistentCapability)
          node.getOrElse(throw new NoNodesAvailableException("No node available that can handle the request: %s".format(request)))
        })

      doSendRequest(Request(request, node, is, os, None))
    }
  }

  private[client] def retryCallback[RequestMsg, ResponseMsg](underlying: Either[Throwable, ResponseMsg] => Unit, maxRetry: Int, capability: Option[Long], persistentCapability: Option[Long])(res: Either[Throwable, ResponseMsg])
  (implicit is: InputSerializer[RequestMsg, ResponseMsg], os: OutputSerializer[RequestMsg, ResponseMsg]): Unit = {
    def propagate(t: Throwable) { underlying(Left(t)) }
    def handleFailure(t: Throwable) {
      t match {
        case ra: RequestAccess[Request[RequestMsg, ResponseMsg]] =>
          log.info("Caught exception(%s) for %s".format(t, ra.request))
          val request = ra.request
          if (request.retryAttempt < maxRetry) {
            try {
              val node = loadBalancer.getOrElse(throw new ClusterDisconnectedException).fold(ex => throw ex, lb => lb.nextNode(capability, persistentCapability).getOrElse(throw new NoNodesAvailableException("No node available that can handle the request: %s".format(request.message))))
              if (!node.equals(request.node)) { // simple check; partitioned version does retry here as well
                val request1 = Request(request.message, node, is, os, Some(retryCallback[RequestMsg, ResponseMsg](underlying, maxRetry, capability, persistentCapability) _), request.retryAttempt + 1)
                log.debug("Resend %s".format(request1))
                doSendRequest(request1)
              } else propagate(t)
            } catch {
              case t1: Throwable => propagate(t)  // propagate original ex (t) for now; may capture/chain t1 if useful
            }
          } else propagate(t)
        case _ => propagate(t)
      }
    }
    if (underlying == null)
      throw new NullPointerException
    if (maxRetry <= 0)
      res.fold(t => handleFailure(t), result => underlying(Right(result)))
    else
      res.fold(t => handleFailure(t), result => underlying(Right(result)))
  }

  protected def updateLoadBalancer(nodes: Set[Endpoint]) {
    loadBalancer = if (nodes != null && nodes.size > 0) {
      try {
        Some(Right(loadBalancerFactory.newLoadBalancer(nodes)))
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
}
