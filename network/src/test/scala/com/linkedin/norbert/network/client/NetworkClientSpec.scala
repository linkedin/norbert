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

import loadbalancer.{LoadBalancerFactory, LoadBalancer, LoadBalancerFactoryComponent}
import network.common.{Endpoint, ClusterIoClientComponent, BaseNetworkClientSpecification}
import cluster._
import cluster.ClusterListenerKey._
import network.NoNodesAvailableException

class NetworkClientSpec extends BaseNetworkClientSpecification {
  val networkClient = new NetworkClient with ClusterClientComponent with ClusterIoClientComponent with LoadBalancerFactoryComponent {
    val lb = mock[LoadBalancer]
    val loadBalancerFactory = mock[LoadBalancerFactory]
    val clusterIoClient = mock[ClusterIoClient]
//    val messageRegistry = mock[MessageRegistry]
    val clusterClient = NetworkClientSpec.this.clusterClient

  }

//  networkClient.messageRegistry.contains(any[Message]) returns true

  "NetworkClient" should {
    "provide common functionality" in { sharedFunctionality }

    "throw ClusterDisconnectedException if the cluster is disconnected when a method is called" in {
      networkClient.start

      networkClient.broadcastMessage(request) must throwA[ClusterDisconnectedException]
      networkClient.sendRequestToNode(request, nodes(1)) must throwA[ClusterDisconnectedException]
      networkClient.sendRequest(request) must throwA[ClusterDisconnectedException]
      networkClient.sendMessage(request) must throwA[ClusterDisconnectedException]
    }

    "continue to operating with the last known router configuration if the cluster is disconnected" in {
      clusterClient.addListener(any[ClusterListener]) returns ClusterListenerKey(1)
      clusterClient.nodes returns nodeSet

    }

    "throw ClusterShutdownException if the cluster is shut down when a method is called" in {
      networkClient.shutdown

      networkClient.broadcastMessage(request) must throwA[NetworkShutdownException]
      networkClient.sendRequestToNode(request, nodes(1)) must throwA[NetworkShutdownException]
      networkClient.sendRequest(request) must throwA[NetworkShutdownException]
      networkClient.sendMessage(request) must throwA[NetworkShutdownException]
    }

    "send the provided message to the node specified by the load balancer for sendMessage" in {
      clusterClient.nodes returns nodeSet
      clusterClient.isConnected returns true
      networkClient.clusterIoClient.nodesChanged(nodeSet) returns endpoints
      networkClient.loadBalancerFactory.newLoadBalancer(endpoints) returns networkClient.lb
      networkClient.lb.nextNode(None, None) returns Some(nodes(1))
//      doNothing.when(clusterIoClient).sendMessage(node, message, null)

      networkClient.start
      networkClient.sendRequest(request) must notBeNull

      there was one(networkClient.lb).nextNode(None, None)
//      clusterIoClient.sendMessage(node, message, null) was called
    }

    "send the provided message to the node specified by the load balancer for sendRequest with the requested capability " in {
      clusterClient.nodes returns nodeSet
      clusterClient.isConnected returns true
      networkClient.clusterIoClient.nodesChanged(nodeSet) returns endpoints
      networkClient.loadBalancerFactory.newLoadBalancer(endpoints) returns networkClient.lb
      networkClient.lb.nextNode(Some(0x1), Some(2L)) returns Some(nodes(1))

      networkClient.start
      networkClient.sendRequest(request, Some(1L), Some(2L)) must notBeNull

      there was one(networkClient.lb).nextNode(Some(0x1), Some(2L))
      there was no(networkClient.lb).nextNode(None, None)
    }

    "send the provided message to the node specified by the load balancer for sendMessage with the requested capability " in {
      clusterClient.nodes returns nodeSet
      clusterClient.isConnected returns true
      networkClient.clusterIoClient.nodesChanged(nodeSet) returns endpoints
      networkClient.loadBalancerFactory.newLoadBalancer(endpoints) returns networkClient.lb
      networkClient.lb.nextNode(Some(0x1), Some(2L)) returns Some(nodes(1))

      networkClient.start
      networkClient.sendMessage(request, Some(1L), Some(2L)) must notBeNull

      there was one(networkClient.lb).nextNode(Some(0x1), Some(2L))
      there was no(networkClient.lb).nextNode(None, None)
    }

    "retryCallback should propagate server exception to underlying when" in {

      val MAX_RETRY = 3
      var either: Either[Throwable, Ping] = null
      val callback = (e: Either[Throwable, Ping]) => either = e

      "exception does not provide RequestAccess" in {
        networkClient.retryCallback[Ping, Ping](callback, 0, None, None)(Left(new Exception))
        either must notBeNull
        either.isLeft must beTrue
      }

      "request.retryAttempt >= maxRetry" in {
        val req: Request[Ping, Ping] = spy(Request[Ping, Ping](null, null, null, null, Some(callback), MAX_RETRY))
        val ra: Exception with RequestAccess[Request[Ping, Ping]] = new Exception with RequestAccess[Request[Ping, Ping]] {
          def request = req
        }
        networkClient.retryCallback[Ping, Ping](callback, MAX_RETRY, None, None)(Left(ra))
        either must notBeNull
        either.isLeft must beTrue
        either.left.get mustEq ra
      }

      "cannot locate next available node" in {
        clusterClient.nodes returns nodeSet
        clusterClient.isConnected returns true
        networkClient.clusterIoClient.nodesChanged(nodeSet) returns endpoints
        networkClient.loadBalancerFactory.newLoadBalancer(endpoints) returns networkClient.lb
        networkClient.lb.nextNode returns None
        networkClient.start

        networkClient.retryCallback[Ping, Ping](callback, MAX_RETRY, None, None)(Left(new RemoteException("FooClass", "ServerError")))
        either must notBeNull
        either.isLeft must beTrue
        either.left.get must haveClass[RemoteException]
      }

      "next node is same as failing node" in {
        clusterClient.nodes returns nodeSet
        clusterClient.isConnected returns true
        networkClient.clusterIoClient.nodesChanged(nodeSet) returns endpoints
        networkClient.loadBalancerFactory.newLoadBalancer(endpoints) returns networkClient.lb
        networkClient.lb.nextNode returns Some(nodes(1))

        networkClient.start

        var req: Request[Ping, Ping] = spy(Request[Ping, Ping](null, nodes(1), null, null, Some(callback)))
        val ra: Exception with RequestAccess[Request[Ping, Ping]] = new Exception with RequestAccess[Request[Ping, Ping]] {
          def request = req
        }

        networkClient.retryCallback[Ping, Ping](callback, MAX_RETRY, None, None)(Left(ra))
        either must notBeNull
        either.isLeft must beTrue
        either.left.get mustEq ra
      }

      "sendMessage: MAX_RETRY reached" in {
        var either: Either[Throwable, Ping] = null
        val callback = (e: Either[Throwable, Ping]) => either = e
        val networkClient2 = new NetworkClient with ClusterClientComponent with ClusterIoClientComponent with LoadBalancerFactoryComponent {
          val lb = new LoadBalancer {
            val iter = NetworkClientSpec.this.nodes.iterator
            def nextNode(capability: Option[Long], permanentCapability: Option[Long]) = Some(iter.next)
          }
          val loadBalancerFactory = mock[LoadBalancerFactory]
          val clusterIoClient = new ClusterIoClient {
            var invocationCount: Int = 0
            def sendMessage[RequestMsg, ResponseMsg](node: Node, requestCtx: Request[RequestMsg, ResponseMsg]) {
              invocationCount += 1
              requestCtx.onFailure(new Exception with RequestAccess[Request[RequestMsg, ResponseMsg]] {
                def request = requestCtx
              })
            }
            def nodesChanged(nodes: Set[Node]) = {NetworkClientSpec.this.endpoints}
            def shutdown {}
          }
          val clusterClient = NetworkClientSpec.this.clusterClient
        }
        networkClient2.clusterClient.nodes returns nodeSet
        networkClient2.clusterClient.isConnected returns true
        networkClient2.loadBalancerFactory.newLoadBalancer(endpoints) returns networkClient2.lb
        networkClient2.start
        networkClient2.sendRequest(request, callback, MAX_RETRY)
        networkClient2.clusterIoClient.invocationCount mustEqual MAX_RETRY
        either mustNotBe null
        either.isLeft must beTrue
      }
    }

    "throw InvalidClusterException if there is no load balancer instance when sendRequest is called" in {
      clusterClient.nodes returns nodeSet
      clusterClient.isConnected returns true
      networkClient.clusterIoClient.nodesChanged(nodeSet) returns endpoints
      networkClient.loadBalancerFactory.newLoadBalancer(endpoints) throws new InvalidClusterException("")
      //      doNothing.when(clusterIoClient).sendMessage(node, message, null)

      networkClient.start
      networkClient.sendRequest(request) must throwA[InvalidClusterException]

      //      clusterIoClient.sendMessage(node, message, null) wasnt called
    }

    "throw InvalidClusterException if there is no load balancer instance when sendMessage is called" in {
      clusterClient.nodes returns nodeSet
      clusterClient.isConnected returns true
      networkClient.clusterIoClient.nodesChanged(nodeSet) returns endpoints
      networkClient.loadBalancerFactory.newLoadBalancer(endpoints) throws new InvalidClusterException("")
      //      doNothing.when(clusterIoClient).sendMessage(node, message, null)

      networkClient.start
      networkClient.sendMessage(request) must throwA[InvalidClusterException]

      //      clusterIoClient.sendMessage(node, message, null) wasnt called
    }

    "throw NoSuchNodeException if load balancer returns None when sendRequest is called" in {
      clusterClient.nodes returns nodeSet
      clusterClient.isConnected returns true
      networkClient.clusterIoClient.nodesChanged(nodeSet) returns endpoints
      networkClient.loadBalancerFactory.newLoadBalancer(endpoints) returns networkClient.lb
      networkClient.lb.nextNode(None, None) returns None
      //      doNothing.when(clusterIoClient).sendMessage(node, message, null)

      networkClient.start
      networkClient.sendRequest(request) must throwA[NoNodesAvailableException]

      there was one(networkClient.lb).nextNode(None, None)
      //      clusterIoClient.sendMessage(node, message, null) wasnt called
    }

    "throw NoSuchNodeException if load balancer returns None when sendMessage is called" in {
      clusterClient.nodes returns nodeSet
      clusterClient.isConnected returns true
      networkClient.clusterIoClient.nodesChanged(nodeSet) returns endpoints
      networkClient.loadBalancerFactory.newLoadBalancer(endpoints) returns networkClient.lb
      networkClient.lb.nextNode(None, None) returns None
      //      doNothing.when(clusterIoClient).sendMessage(node, message, null)

      networkClient.start
      networkClient.sendMessage(request) must throwA[NoNodesAvailableException]

      there was one(networkClient.lb).nextNode(None, None)
      //      clusterIoClient.sendMessage(node, message, null) wasnt called
    }

  }
}
