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
import netty.NettyNetworkClient
import network.common._
import runtime.BoxedUnit
import com.linkedin.norbert.network.javaobjects.{NodeSpecification => JNodeSpecification, PartitionedNodeSpecification => JPartitionedNodeSpecification,
                                                RetrySpecification => JRetrySpecification, PartitionedRetrySpecification => JPartitionedRetrySpecification,
                                                RequestSpecification => JRequestSpecification, PartitionedRequestSpecification => JPartitionedRequestSpecification}
import com.linkedin.norbert.network.UnitConversions


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
  @deprecated("Use sendRequest(RequestSpecification[RequestMsg], NodeSpecification, RetrySpecification[ResponseMsg]), 12/17/2014")
  def sendRequest[RequestMsg, ResponseMsg](request: RequestMsg, callback: Either[Throwable, ResponseMsg] => Unit)
  (implicit is: InputSerializer[RequestMsg, ResponseMsg], os: OutputSerializer[RequestMsg, ResponseMsg]): Unit =  {
    val requestSpec = RequestSpecification(request)
    val nodeSpec = new NodeSpecification().build
    val retrySpec = RetrySpecification(0, Some(callback))
    sendRequest(requestSpec, nodeSpec, retrySpec)
  }

  @deprecated("Use sendRequest(RequestSpecification[RequestMsg], NodeSpecification, RetrySpecification[ResponseMsg]), 12/17/2014")
  def sendRequest[RequestMsg, ResponseMsg](request: RequestMsg, callback: Either[Throwable, ResponseMsg] => Unit, capability: Option[Long])
  (implicit is: InputSerializer[RequestMsg, ResponseMsg], os: OutputSerializer[RequestMsg, ResponseMsg]): Unit = {
    val requestSpec = RequestSpecification(request)
    val nodeSpec = new NodeSpecification().setCapability(capability).build
    val retrySpec = RetrySpecification(0, Some(callback))
    sendRequest(requestSpec, nodeSpec, retrySpec)
  }

  @deprecated("Use sendRequest(RequestSpecification[RequestMsg], NodeSpecification, RetrySpecification[ResponseMsg]), 12/17/2014")
  def sendRequest[RequestMsg, ResponseMsg](request: RequestMsg, callback: Either[Throwable, ResponseMsg] => Unit, capability: Option[Long], persistentCapability: Option[Long])
  (implicit is: InputSerializer[RequestMsg, ResponseMsg], os: OutputSerializer[RequestMsg, ResponseMsg]): Unit =  doIfConnected {
     val requestSpec = RequestSpecification(request)
     val nodeSpec = new NodeSpecification().setCapability(capability).setPersistentCapability(persistentCapability).build
     val retrySpec = RetrySpecification(0, Some(callback))
     sendRequest(requestSpec, nodeSpec, retrySpec)
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
  @deprecated("Use sendRequest(RequestSpecification[RequestMsg], NodeSpecification, RetrySpecification[ResponseMsg]), 12/17/2014")
  def sendRequest[RequestMsg, ResponseMsg](request: RequestMsg)
  (implicit is: InputSerializer[RequestMsg, ResponseMsg], os: OutputSerializer[RequestMsg, ResponseMsg]): Future[ResponseMsg] = {
    val future = new FutureAdapterListener[ResponseMsg]
    val requestSpec = RequestSpecification(request)
    val nodeSpec = new NodeSpecification().build
    val retrySpec = RetrySpecification(0, Some(future))
    sendRequest(requestSpec, nodeSpec, retrySpec)
    future
  }

  @deprecated("Use sendRequest(RequestSpecification[RequestMsg], NodeSpecification, RetrySpecification[ResponseMsg]), 12/17/2014")
  def sendRequest[RequestMsg, ResponseMsg](request: RequestMsg, maxRetry:Int)
  (implicit is: InputSerializer[RequestMsg, ResponseMsg], os: OutputSerializer[RequestMsg, ResponseMsg]): Future[ResponseMsg] = {
    val future = new FutureAdapterListener[ResponseMsg]
    val requestSpec = RequestSpecification(request)
    val nodeSpec = new NodeSpecification().build
    val retrySpec = RetrySpecification(maxRetry, Some(future))
    sendRequest(requestSpec, nodeSpec, retrySpec)
    future
  }

  @deprecated("Use sendRequest(RequestSpecification[RequestMsg], NodeSpecification, RetrySpecification[ResponseMsg]), 12/17/2014")
  def sendRequest[RequestMsg, ResponseMsg](request: RequestMsg, capability: Option[Long])
  (implicit is: InputSerializer[RequestMsg, ResponseMsg], os: OutputSerializer[RequestMsg, ResponseMsg]): Future[ResponseMsg] = {
    val future = new FutureAdapterListener[ResponseMsg]
    val requestSpec = RequestSpecification(request)
    val nodeSpec = new NodeSpecification().setCapability(capability).build
    val retrySpec = RetrySpecification(0, Some(future))
    sendRequest(requestSpec, nodeSpec, retrySpec)
    future
  }

  @deprecated("Use sendRequest(RequestSpecification[RequestMsg], NodeSpecification, RetrySpecification[ResponseMsg]), 12/17/2014")
  def sendRequest[RequestMsg, ResponseMsg](request: RequestMsg, capability: Option[Long], persistentCapability: Option[Long])
  (implicit is: InputSerializer[RequestMsg, ResponseMsg], os: OutputSerializer[RequestMsg, ResponseMsg]): Future[ResponseMsg] = {
    val future = new FutureAdapterListener[ResponseMsg]
    val requestSpec = RequestSpecification(request)
    val nodeSpec = new NodeSpecification().setCapability(capability).setPersistentCapability(persistentCapability).build
    val retrySpec = RetrySpecification(0, Some(future))
    sendRequest(requestSpec, nodeSpec, retrySpec)
    future
  }

  @deprecated("Use sendRequest(RequestSpecification[RequestMsg], NodeSpecification, RetrySpecification[ResponseMsg]), 12/17/2014")
  def sendRequest[RequestMsg, ResponseMsg](request: RequestMsg, maxRetry: Int, capability: Option[Long])
  (implicit is: InputSerializer[RequestMsg, ResponseMsg], os: OutputSerializer[RequestMsg, ResponseMsg]): Future[ResponseMsg] = {
    val future = new FutureAdapterListener[ResponseMsg]
    val requestSpec = RequestSpecification(request)
    val nodeSpec = new NodeSpecification().setCapability(capability).build
    val retrySpec = RetrySpecification(maxRetry, Some(future))
    sendRequest(requestSpec, nodeSpec, retrySpec)
    future
  }

  @deprecated("Use sendRequest(RequestSpecification[RequestMsg], NodeSpecification, RetrySpecification[ResponseMsg]), 12/17/2014")
  def sendRequest[RequestMsg, ResponseMsg](request: RequestMsg, maxRetry: Int, capability: Option[Long], persistentCapability: Option[Long])
  (implicit is: InputSerializer[RequestMsg, ResponseMsg], os: OutputSerializer[RequestMsg, ResponseMsg]): Future[ResponseMsg] = {
    val future = new FutureAdapterListener[ResponseMsg]
    val requestSpec = RequestSpecification(request)
    val nodeSpec = new NodeSpecification().setCapability(capability).setPersistentCapability(persistentCapability).build
    val retrySpec = RetrySpecification(maxRetry, Some(future))
    sendRequest(requestSpec, nodeSpec, retrySpec)
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
  @deprecated("Use sendRequest(RequestSpecification[RequestMsg], NodeSpecification, RetrySpecification[ResponseMsg]), 12/17/2014")
  def sendRequest[RequestMsg, ResponseMsg](request: RequestMsg, callback: Either[Throwable, ResponseMsg] => Unit, maxRetry: Int)
  (implicit is: InputSerializer[RequestMsg, ResponseMsg], os: OutputSerializer[RequestMsg, ResponseMsg]): Unit = {
    val requestSpec = RequestSpecification(request)
    val nodeSpec = new NodeSpecification().build
    val retrySpec = RetrySpecification(maxRetry, Some(callback))
    sendRequest(requestSpec, nodeSpec, retrySpec)
  }

  @deprecated("Use sendRequest(RequestSpecification[RequestMsg], NodeSpecification, RetrySpecification[ResponseMsg]), 12/17/2014")
  def sendRequest[RequestMsg, ResponseMsg](request: RequestMsg, callback: Either[Throwable, ResponseMsg] => Unit, maxRetry: Int, capability: Option[Long])
  (implicit is: InputSerializer[RequestMsg, ResponseMsg], os:OutputSerializer[RequestMsg, ResponseMsg]): Unit = {
    val requestSpec = RequestSpecification(request)
    val nodeSpec = new NodeSpecification().setCapability(capability).build
    val retrySpec = RetrySpecification(maxRetry, Some(callback))
    sendRequest(requestSpec, nodeSpec, retrySpec)
  }

  @deprecated("Use sendRequest(RequestSpecification[RequestMsg], NodeSpecification, RetrySpecification[ResponseMsg]), 12/17/2014")
  def sendRequest[RequestMsg, ResponseMsg](request: RequestMsg, callback: Either[Throwable, ResponseMsg] => Unit, maxRetry: Int, capability: Option[Long], persistentCapability: Option[Long])
                                          (implicit is: InputSerializer[RequestMsg, ResponseMsg], os: OutputSerializer[RequestMsg, ResponseMsg]): Unit = doIfConnected {
    // TODO: This still reroutes to the old sendRequest method so that we can test with it
    // TODO: Uncomment the below when we are completely satisfied to reroute it to the new sendRequest method.
//    val requestSpec = RequestSpecification(request)
//    val nodeSpec = new NodeSpecification().setCapability(capability).setPersistentCapability(persistentCapability).build
//    val retrySpec = RetrySpecification(maxRetry, Some(callback))
//    sendRequest(requestSpec, nodeSpec, retrySpec)
    if (request == null) throw new NullPointerException

    val loadBalancerReady = loadBalancer.getOrElse(throw new ClusterDisconnectedException("Client has no node information"))

    val node = loadBalancerReady.fold(ex => throw ex,
      lb => {
        val node: Option[Node] = lb.nextNode(capability, persistentCapability)
        node.getOrElse(throw new NoNodesAvailableException("No node available that can handle the request: %s".format(request)))
      })

    doSendRequest(Request(request, node, is, os, if (maxRetry == 0) Some(callback) else Some(retryCallback[RequestMsg, ResponseMsg](callback, maxRetry, capability, persistentCapability) _)))
  }

  /**
   * New sendRequest API. Functionally the same as the old one, but this takes three wrapper objects as input:
   * requestSpec: Handles the request object.
   * nodeSpec: Handles capability and persistent capability for the node.
   * retrySpec: Handles the maxRetry and callback used for the retry function.
   *
   * These wrapper objects contain overloading and/or defaulting to simplify development;
   * instead of adding new overloaded sendRequest methods, changes should be made to the
   * wrapper objects whenever possible.
   */


  def sendRequest[RequestMsg, ResponseMsg](requestSpec: JRequestSpecification[RequestMsg], nodeSpec: JNodeSpecification, retrySpec: JRetrySpecification[ResponseMsg])
  (implicit is: InputSerializer[RequestMsg, ResponseMsg], os:OutputSerializer[RequestMsg, ResponseMsg]): Unit = doIfConnected {
    if (requestSpec.getMessage() == null) throw new NullPointerException
    // Convert return type of callback from BoxedUnit to Unit
    val cb = retrySpec.getCallback();
    val unitConversion = new UnitConversions[ResponseMsg]
    val callback = unitConversion.uncurryImplicitly(cb)


    val loadBalancerReady = loadBalancer.getOrElse(throw new ClusterDisconnectedException("Client has no node information"))

    // Convert capability and persistentCapability from java.lang.Long to scala.Long
    val capability = Option(Long.unbox(nodeSpec.getCapability()))
    val persistentCapability = Option(Long.unbox(nodeSpec.getPersistentCapability()))

    val node = loadBalancerReady.fold(ex => throw ex,
      lb => {
        val node: Option[Node] = lb.nextNode(capability, persistentCapability)
        node.getOrElse(throw new NoNodesAvailableException("No node available that can handle the request: %s".format(requestSpec.getMessage())))
      })
    doSendRequest(Request(requestSpec.getMessage(), node, is, os, if (retrySpec.getMaxRetry() == 0) Some(callback) else Some(retryCallback[RequestMsg, ResponseMsg](callback, retrySpec.getMaxRetry(), capability, persistentCapability) _)))
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

  /**
   * Sends the alternative one way message to a node in the cluster. The <code>NetworkClient</code> defers to the current
   * <code>LoadBalancer</code> to decide which <code>Node</code> the request should be sent to.
   *
   * @param request the message to send
   *
   * @throws InvalidClusterException thrown if the cluster is currently in an invalid state
   * @throws NoNodesAvailableException thrown if the <code>LoadBalancer</code> was unable to provide a <code>Node</code>
   * to send the request to
   * @throws ClusterDisconnectedException thrown if the cluster is not connected when the method is called
   */

  def sendAltMessage[RequestMsg, ResponseMsg](request: RequestMsg)
                                          (implicit is: InputSerializer[RequestMsg, ResponseMsg], os: OutputSerializer[RequestMsg, ResponseMsg]) {
    doIfConnected {
      sendAltMessage(request, None, None)
    }
  }

  def sendAltMessage[RequestMsg, ResponseMsg](request: RequestMsg, capability: Option[Long])
                                          (implicit is: InputSerializer[RequestMsg, ResponseMsg], os: OutputSerializer[RequestMsg, ResponseMsg]) {
    doIfConnected {
      sendAltMessage(request, capability, None)
    }
  }

  def sendAltMessage[RequestMsg, ResponseMsg](request: RequestMsg, capability: Option[Long], persistentCapability: Option[Long])
                                          (implicit is: InputSerializer[RequestMsg, ResponseMsg], os: OutputSerializer[RequestMsg, ResponseMsg]) {
    doIfConnected {
      if (request == null) throw new NullPointerException

      val loadBalancerReady = loadBalancer.getOrElse(throw new ClusterDisconnectedException("Client has no node information"))

      val node = loadBalancerReady.fold(ex => throw ex,
        lb => {
          val node: Option[Node] = lb.nextNode(capability, persistentCapability)
          node.getOrElse(throw new NoNodesAvailableException("No node available that can handle the request: %s".format(request)))
        })

      doSendAltMessage(BaseRequest(request, node, is, os))
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
