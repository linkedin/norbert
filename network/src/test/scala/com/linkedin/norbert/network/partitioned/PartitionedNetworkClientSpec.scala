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

import common.{Endpoint, ClusterIoClientComponent, BaseNetworkClientSpecification}
import loadbalancer._
import java.util.concurrent.ExecutionException
import cluster.{Node, InvalidClusterException, ClusterDisconnectedException, ClusterClientComponent}
import scala.Left
import scala.Some

class PartitionedNetworkClientSpec extends BaseNetworkClientSpecification {
  val networkClient = new PartitionedNetworkClient[Int] with ClusterClientComponent with ClusterIoClientComponent
      with PartitionedLoadBalancerFactoryComponent[Int] {
    val lb = mock[PartitionedLoadBalancer[Int]]
    val loadbalancer = new PartitionedLoadBalancer[Int] {
      def nextNode(id: Int, capability: Option[Long], permanentCapability: Option[Long]) = lb.nextNode(id, capability, permanentCapability)
      def nodesForOneReplica(id: Int, capability: Option[Long], permanentCapability: Option[Long]) = lb.nodesForOneReplica(id, capability, permanentCapability)
      def nodesForPartitionedId(id: Int, capability: Option[Long], permanentCapability: Option[Long]) = lb.nodesForPartitionedId(id, capability, permanentCapability)
      def nodesForPartitions(id: Int, partitions: Set[Int], capability: Option[Long], permanentCapability: Option[Long]) = lb.nodesForPartitions(id, partitions, capability, permanentCapability)
    }
    val loadBalancerFactory = mock[PartitionedLoadBalancerFactory[Int]]
    val clusterIoClient = mock[ClusterIoClient]
    val clusterClient = PartitionedNetworkClientSpec.this.clusterClient
  }

//  networkClient.messageRegistry.contains(any[Message]) returns true

  "PartitionedNetworkClient" should {
    "provide common functionality" in { sharedFunctionality }

    "throw ClusterDisconnectedException if the cluster is disconnected when a method is called" in {
      networkClient.start

      networkClient.broadcastMessage(request) must throwA[ClusterDisconnectedException]
      networkClient.sendRequestToNode(request, nodes(1)) must throwA[ClusterDisconnectedException]
      networkClient.sendRequest(1, request) must throwA[ClusterDisconnectedException]
      networkClient.sendRequest(Set(1, 2), request) must throwA[ClusterDisconnectedException]
      networkClient.sendMessage(1, request) must throwA[ClusterDisconnectedException]
      networkClient.sendMessage(Set(1, 2), request) must throwA[ClusterDisconnectedException]
    }

    "throw ClusterShutdownException if the cluster is shut down when a method is called" in {
      networkClient.shutdown

      networkClient.broadcastMessage(request) must throwA[NetworkShutdownException]
      networkClient.sendRequestToNode(request, nodes(1)) must throwA[NetworkShutdownException]
      networkClient.sendRequest(1, request) must throwA[NetworkShutdownException]
      networkClient.sendRequest(Set(1, 2), request) must throwA[NetworkShutdownException]
      networkClient.sendMessage(1, request) must throwA[NetworkShutdownException]
      networkClient.sendMessage(Set(1, 2), request) must throwA[NetworkShutdownException]
    }

//    "throw an InvalidMessageException if an unregistered message is sent" in {
//      clusterClient.nodes returns nodeSet
//      clusterClient.isConnected returns true
////      networkClient.messageRegistry.contains(any[Message]) returns false
//
//      networkClient.start
//
//      networkClient.broadcastMessage(request) must throwA[InvalidMessageException]
//      networkClient.sendRequestToNode(request, nodes(1)) must throwA[InvalidMessageException]
//      networkClient.sendRequest(1, request) must throwA[InvalidMessageException]
//      networkClient.sendRequest(Set(1, 2), request) must throwA[InvalidMessageException]
//    }

    "when sendRequest(id, message) is called" in {
      "send the provided message to the node specified by the load balancer for sendRequest" in {
        clusterClient.nodes returns nodeSet
        clusterClient.isConnected returns true
        networkClient.clusterIoClient.nodesChanged(nodeSet) returns endpoints
        networkClient.loadBalancerFactory.newLoadBalancer(endpoints) returns networkClient.loadbalancer
        networkClient.lb.nextNode(1, None, None) returns Some(nodes(1))
        //      doNothing.when(clusterIoClient).sendRequest(node, message, null)

        networkClient.start
        networkClient.sendRequest(1, request) must notBeNull

        there was one(networkClient.lb).nextNode(1, None, None)
        //      clusterIoClient.sendRequest(node, message, null) was called
      }

      "send the provided message to the node specified by the load balancer for sendMessage" in {
        clusterClient.nodes returns nodeSet
        clusterClient.isConnected returns true
        networkClient.clusterIoClient.nodesChanged(nodeSet) returns endpoints
        networkClient.loadBalancerFactory.newLoadBalancer(endpoints) returns networkClient.loadbalancer
        networkClient.lb.nextNode(1, None, None) returns Some(nodes(1))
        //      doNothing.when(clusterIoClient).sendRequest(node, message, null)

        networkClient.start
        networkClient.sendMessage(1, request) must notBeNull

        there was one(networkClient.lb).nextNode(1, None, None)
        //      clusterIoClient.sendRequest(node, message, null) was called
      }

      "send the provided message to the node specified by the load balancer filtering based on capability for sendRequest" in {
        clusterClient.nodes returns nodeSet
        clusterClient.isConnected returns true
        networkClient.clusterIoClient.nodesChanged(nodeSet) returns endpoints
        networkClient.loadBalancerFactory.newLoadBalancer(endpoints) returns networkClient.loadbalancer
        networkClient.lb.nextNode(1, Some(0x1), Some(0x2)) returns Some(nodes(1))

        networkClient.start
        networkClient.sendRequest(1, request, Some(0x1L), Some(2L)) must notBeNull

        there was one(networkClient.lb).nextNode(1, Some(0x1L), Some(2L))
        there was no(networkClient.lb).nextNode(1, None, None)
      }

      "send the provided message to the node specified by the load balancer filtering based on capability fir sendMessage" in {
        clusterClient.nodes returns nodeSet
        clusterClient.isConnected returns true
        networkClient.clusterIoClient.nodesChanged(nodeSet) returns endpoints
        networkClient.loadBalancerFactory.newLoadBalancer(endpoints) returns networkClient.loadbalancer
        networkClient.lb.nextNode(1, Some(0x1), Some(0x2)) returns Some(nodes(1))

        networkClient.start
        networkClient.sendMessage(1, request, Some(0x1L), Some(2L)) must notBeNull

        there was one(networkClient.lb).nextNode(1, Some(0x1L), Some(2L))
        there was no(networkClient.lb).nextNode(1, None, None)
      }

      "throw InvalidClusterException if there is no load balancer instance when sendRequest is called" in {
        clusterClient.nodes returns nodeSet
        clusterClient.isConnected returns true
        networkClient.clusterIoClient.nodesChanged(nodeSet) returns endpoints
        networkClient.loadBalancerFactory.newLoadBalancer(endpoints) throws new InvalidClusterException("")
        //      doNothing.when(clusterIoClient).sendRequest(node, message, null)

        networkClient.start
        networkClient.sendRequest(1, request) must throwA[InvalidClusterException]

        //      clusterIoClient.sendRequest(node, message, null) wasnt called
      }

      "throw InvalidClusterException if there is no load balancer instance when sendMessage is called" in {
        clusterClient.nodes returns nodeSet
        clusterClient.isConnected returns true
        networkClient.clusterIoClient.nodesChanged(nodeSet) returns endpoints
        networkClient.loadBalancerFactory.newLoadBalancer(endpoints) throws new InvalidClusterException("")
        //      doNothing.when(clusterIoClient).sendRequest(node, message, null)

        networkClient.start
        networkClient.sendMessage(1, request) must throwA[InvalidClusterException]

        //      clusterIoClient.sendRequest(node, message, null) wasnt called
      }

      "throw NoSuchNodeException if load balancer returns None when sendRequest is called" in {
        clusterClient.nodes returns nodeSet
        clusterClient.isConnected returns true
        networkClient.clusterIoClient.nodesChanged(nodeSet) returns endpoints
        networkClient.loadBalancerFactory.newLoadBalancer(endpoints) returns networkClient.lb
        networkClient.lb.nextNode(1, None, None) returns None
        //      doNothing.when(clusterIoClient).sendRequest(node, message, null)

        networkClient.start
        networkClient.sendRequest(1, request) must throwA[NoNodesAvailableException]

        there was one(networkClient.lb).nextNode(1, None, None)
        //      clusterIoClient.sendRequest(node, message, null) wasnt called
      }

      "throw NoSuchNodeException if load balancer returns None when sendMessage is called" in {
        clusterClient.nodes returns nodeSet
        clusterClient.isConnected returns true
        networkClient.clusterIoClient.nodesChanged(nodeSet) returns endpoints
        networkClient.loadBalancerFactory.newLoadBalancer(endpoints) returns networkClient.lb
        networkClient.lb.nextNode(1, None, None) returns None
        //      doNothing.when(clusterIoClient).sendRequest(node, message, null)

        networkClient.start
        networkClient.sendMessage(1, request) must throwA[NoNodesAvailableException]

        there was one(networkClient.lb).nextNode(1, None, None)
        //      clusterIoClient.sendRequest(node, message, null) wasnt called
      }
    }

    "when sendRequest(ids, message) is called" in {
      "send the provided message to the node specified by the load balancer" in {
        clusterClient.nodes returns nodeSet
        clusterClient.isConnected returns true
        networkClient.clusterIoClient.nodesChanged(nodeSet) returns endpoints
        networkClient.loadBalancerFactory.newLoadBalancer(endpoints) returns networkClient.loadbalancer
        networkClient.lb.nextNode(1, None, None) returns Some(nodes(1))
        networkClient.lb.nextNode(2, None, None) returns Some(nodes(2))
        networkClient.lb.nextNode(3, None, None) returns Some(nodes(1))

        //      doNothing.when(clusterIoClient).sendRequest(node, message, null)

        networkClient.start
        networkClient.sendRequest(Set(1, 2, 3), request) must notBeNull

        got {
          one(networkClient.lb).nextNode(1, None, None)
          one(networkClient.lb).nextNode(2, None, None)
          one(networkClient.lb).nextNode(3, None, None)
        }
        //      clusterIoClient.sendRequest(node, message, null) was called
      }

      "send the provided one way message to the node specified by the load balancer" in {
        clusterClient.nodes returns nodeSet
        clusterClient.isConnected returns true
        networkClient.clusterIoClient.nodesChanged(nodeSet) returns endpoints
        networkClient.loadBalancerFactory.newLoadBalancer(endpoints) returns networkClient.loadbalancer
        networkClient.lb.nextNode(1, None, None) returns Some(nodes(1))
        networkClient.lb.nextNode(2, None, None) returns Some(nodes(2))
        networkClient.lb.nextNode(3, None, None) returns Some(nodes(1))

        //      doNothing.when(clusterIoClient).sendRequest(node, message, null)

        networkClient.start
        networkClient.sendMessage(Set(1, 2, 3), request) must notBeNull

        got {
          one(networkClient.lb).nextNode(1, None, None)
          one(networkClient.lb).nextNode(2, None, None)
          one(networkClient.lb).nextNode(3, None, None)
        }
        //      clusterIoClient.sendRequest(node, message, null) was called
      }

      "send the provided message to the node specified by the load balancer with capability filter" in {
        clusterClient.nodes returns nodeSet
        clusterClient.isConnected returns true
        networkClient.clusterIoClient.nodesChanged(nodeSet) returns endpoints
        networkClient.loadBalancerFactory.newLoadBalancer(endpoints) returns networkClient.loadbalancer
        networkClient.lb.nextNode(1, Some(0xffL), Some(2L)) returns Some(nodes(1))
        networkClient.lb.nextNode(2, Some(0xffL), Some(2L)) returns Some(nodes(2))
        networkClient.lb.nextNode(3, Some(0xffL), Some(2L)) returns Some(nodes(1))

        //      doNothing.when(clusterIoClient).sendRequest(node, message, null)

        networkClient.start
        networkClient.sendRequest(Set(1, 2, 3), request, Some(0xffL), Some(2L)) must notBeNull

        got {
          no(networkClient.lb).nextNode(1, None, None)
          no(networkClient.lb).nextNode(2, None, None)
          no(networkClient.lb).nextNode(3, None, None)
          one(networkClient.lb).nextNode(1, Some(0xffL), Some(2L))
          one(networkClient.lb).nextNode(2, Some(0xffL), Some(2L))
          one(networkClient.lb).nextNode(3, Some(0xffL), Some(2L))
        }
        //      clusterIoClient.sendRequest(node, message, null) was called
      }


      "send the provided one way message to the node specified by the load balancer with capability filter" in {
        clusterClient.nodes returns nodeSet
        clusterClient.isConnected returns true
        networkClient.clusterIoClient.nodesChanged(nodeSet) returns endpoints
        networkClient.loadBalancerFactory.newLoadBalancer(endpoints) returns networkClient.loadbalancer
        networkClient.lb.nextNode(1, Some(0xffL), Some(2L)) returns Some(nodes(1))
        networkClient.lb.nextNode(2, Some(0xffL), Some(2L)) returns Some(nodes(2))
        networkClient.lb.nextNode(3, Some(0xffL), Some(2L)) returns Some(nodes(1))

        //      doNothing.when(clusterIoClient).sendRequest(node, message, null)

        networkClient.start
        networkClient.sendMessage(Set(1, 2, 3), request, Some(0xffL), Some(2L)) must notBeNull

        got {
          no(networkClient.lb).nextNode(1, None, None)
          no(networkClient.lb).nextNode(2, None, None)
          no(networkClient.lb).nextNode(3, None, None)
          one(networkClient.lb).nextNode(1, Some(0xffL), Some(2L))
          one(networkClient.lb).nextNode(2, Some(0xffL), Some(2L))
          one(networkClient.lb).nextNode(3, Some(0xffL), Some(2L))
        }
        //      clusterIoClient.sendRequest(node, message, null) was called
      }


      "throw InvalidClusterException if there is no load balancer instance when sendRequest is called" in {
        clusterClient.nodes returns nodeSet
        clusterClient.isConnected returns true
        networkClient.clusterIoClient.nodesChanged(nodeSet) returns endpoints
        networkClient.loadBalancerFactory.newLoadBalancer(endpoints) throws new InvalidClusterException("")
        //      doNothing.when(clusterIoClient).sendRequest(node, message, null)

        networkClient.start
        networkClient.sendRequest(Set(1, 2, 3), request) must throwA[InvalidClusterException]

        //      clusterIoClient.sendRequest(node, message, null) wasnt called
      }

      "throw InvalidClusterException if there is no load balancer instance when sendMessage is called" in {
        clusterClient.nodes returns nodeSet
        clusterClient.isConnected returns true
        networkClient.clusterIoClient.nodesChanged(nodeSet) returns endpoints
        networkClient.loadBalancerFactory.newLoadBalancer(endpoints) throws new InvalidClusterException("")
        //      doNothing.when(clusterIoClient).sendRequest(node, message, null)

        networkClient.start
        networkClient.sendMessage(Set(1, 2, 3), request) must throwA[InvalidClusterException]

        //      clusterIoClient.sendRequest(node, message, null) wasnt called
      }

      "throw NoSuchNodeException if load balancer returns None when sendRequest is called" in {
        clusterClient.nodes returns nodeSet
        clusterClient.isConnected returns true
        networkClient.clusterIoClient.nodesChanged(nodeSet) returns endpoints
        networkClient.loadBalancerFactory.newLoadBalancer(endpoints) returns networkClient.loadbalancer
        networkClient.lb.nextNode(1, None, None) returns None
//      doNothing.when(clusterIoClient).sendRequest(node, message, null)

        networkClient.start
        networkClient.sendRequest(Set(1, 2, 3), request) must throwA[NoNodesAvailableException]

        there was one(networkClient.lb).nextNode(1, None, None)
//      clusterIoClient.sendRequest(node, message, null) wasnt called
      }

      "throw NoSuchNodeException if load balancer returns None when sendMessage is called" in {
        clusterClient.nodes returns nodeSet
        clusterClient.isConnected returns true
        networkClient.clusterIoClient.nodesChanged(nodeSet) returns endpoints
        networkClient.loadBalancerFactory.newLoadBalancer(endpoints) returns networkClient.loadbalancer
        networkClient.lb.nextNode(1, None, None) returns None
        //      doNothing.when(clusterIoClient).sendRequest(node, message, null)

        networkClient.start
        networkClient.sendMessage(Set(1, 2, 3), request) must throwA[NoNodesAvailableException]

        there was one(networkClient.lb).nextNode(1, None, None)
        //      clusterIoClient.sendRequest(node, message, null) wasnt called
      }
    }


  "when sendRequest(ids, message, messageCustomizer) is called" in {
    "send the provided message to the node specified by the load balancer" in {
      clusterClient.nodes returns nodeSet
      clusterClient.isConnected returns true
      networkClient.clusterIoClient.nodesChanged(nodeSet) returns endpoints
      networkClient.loadBalancerFactory.newLoadBalancer(endpoints) returns networkClient.loadbalancer
      List(1, 2, 3).foreach(networkClient.lb.nextNode(_, None, None) returns Some(Node(1, "localhost:31313", true)))

      networkClient.start
      networkClient.sendRequest(Set(1, 2, 3), messageCustomizer _)

      List(1, 2, 3).foreach(there was one(networkClient.lb).nextNode(_, None, None))

      List(1, 2, 3).foreach(networkClient.lb.nextNode(_, Some(0x3L), Some(0L)) returns Some(Node(1, "localhost:31313", true)))
      networkClient.sendRequest(Set(1, 2, 3), messageCustomizer _, Some(0x3L), Some(0L))
      List(1, 2, 3).foreach(there was one(networkClient.lb).nextNode(_,Some(0x3L), Some(0L)))
    }

    "send the provided one way message to the node specified by the load balancer" in {
      clusterClient.nodes returns nodeSet
      clusterClient.isConnected returns true
      networkClient.clusterIoClient.nodesChanged(nodeSet) returns endpoints
      networkClient.loadBalancerFactory.newLoadBalancer(endpoints) returns networkClient.loadbalancer
      List(1, 2, 3).foreach(networkClient.lb.nextNode(_, None, None) returns Some(Node(1, "localhost:31313", true)))

      networkClient.start
      networkClient.sendMessage(Set(1, 2, 3), messageCustomizer _)

      List(1, 2, 3).foreach(there was one(networkClient.lb).nextNode(_, None, None))

      List(1, 2, 3).foreach(networkClient.lb.nextNode(_, Some(0x3L), Some(0L)) returns Some(Node(1, "localhost:31313", true)))
      networkClient.sendMessage(Set(1, 2, 3), messageCustomizer _, Some(0x3L), Some(0L))
      List(1, 2, 3).foreach(there was one(networkClient.lb).nextNode(_,Some(0x3L), Some(0L)))
    }

    "call the message customizer" in {
      var callCount = 0
      var nodeMap = Map[Node, Set[Int]]()

      def mc(node: Node, ids: Set[Int]): Ping = {
        callCount += 1
        nodeMap = nodeMap + (node -> ids)
        new Ping
      }

      clusterClient.nodes returns nodeSet
      clusterClient.isConnected returns true
      networkClient.clusterIoClient.nodesChanged(nodeSet) returns endpoints
      networkClient.loadBalancerFactory.newLoadBalancer(endpoints) returns networkClient.loadbalancer
      List(1, 2).foreach(networkClient.lb.nextNode(_, None, None) returns Some(nodes(0)))
      List(3, 4).foreach(networkClient.lb.nextNode(_, None, None) returns Some(nodes(1)))

      networkClient.start
      networkClient.sendRequest(Set(1, 2, 3, 4), mc _)

      callCount must be_==(2)
      nodeMap.size must be_==(2)
      nodeMap(nodes(0)) must haveTheSameElementsAs(Array(1, 2))
      nodeMap(nodes(1)) must haveTheSameElementsAs(Array(3, 4))
    }

    "call the message customizer (one way message)" in {
      var callCount = 0
      var nodeMap = Map[Node, Set[Int]]()

      def mc(node: Node, ids: Set[Int]): Ping = {
        callCount += 1
        nodeMap = nodeMap + (node -> ids)
        new Ping
      }

      clusterClient.nodes returns nodeSet
      clusterClient.isConnected returns true
      networkClient.clusterIoClient.nodesChanged(nodeSet) returns endpoints
      networkClient.loadBalancerFactory.newLoadBalancer(endpoints) returns networkClient.loadbalancer
      List(1, 2).foreach(networkClient.lb.nextNode(_, None, None) returns Some(nodes(0)))
      List(3, 4).foreach(networkClient.lb.nextNode(_, None, None) returns Some(nodes(1)))

      networkClient.start
      networkClient.sendMessage(Set(1, 2, 3, 4), mc _)

      callCount must be_==(2)
      nodeMap.size must be_==(2)
      nodeMap(nodes(0)) must haveTheSameElementsAs(Array(1, 2))
      nodeMap(nodes(1)) must haveTheSameElementsAs(Array(3, 4))
    }

    "treats an exception from the message customizer as a failed response" in {
      def mc(node: Node, ids: Set[Int]): Ping = {
        throw new Exception
      }

      clusterClient.nodes returns nodeSet
      clusterClient.isConnected returns true
      networkClient.clusterIoClient.nodesChanged(nodeSet) returns endpoints
      networkClient.loadBalancerFactory.newLoadBalancer(endpoints) returns networkClient.loadbalancer
      List(1, 2).foreach(networkClient.lb.nextNode(_, None, None) returns Some(nodes(0)))

      networkClient.start
      val ri = networkClient.sendRequest(Set(1, 2), mc _)
      ri.hasNext must beTrue
      ri.next must throwA[ExecutionException]
    }

      "throw InvalidClusterException if there is no load balancer instance when sendRequest is called" in {
        clusterClient.nodes returns nodeSet
        clusterClient.isConnected returns true
        networkClient.clusterIoClient.nodesChanged(nodeSet) returns endpoints
        networkClient.loadBalancerFactory.newLoadBalancer(endpoints) throws new InvalidClusterException("")
//      doNothing.when(clusterIoClient).sendRequest(node, message, null)

        networkClient.start
        networkClient.sendRequest(Set(1, 2, 3), messageCustomizer _)  must throwA[InvalidClusterException]
        networkClient.sendMessage(Set(1, 2, 3), messageCustomizer _)  must throwA[InvalidClusterException]

//      clusterIoClient.sendRequest(node, message, null) wasnt called
      }

      "throw NoSuchNodeException if load balancer returns None when sendRequest is called" in {
        clusterClient.nodes returns nodeSet
        clusterClient.isConnected returns true
        networkClient.clusterIoClient.nodesChanged(nodeSet) returns endpoints
        networkClient.loadBalancerFactory.newLoadBalancer(endpoints) returns networkClient.loadbalancer
        networkClient.lb.nextNode(1, None, None) returns None
//      doNothing.when(clusterIoClient).sendRequest(node, message, null)

        networkClient.start
        networkClient.sendRequest(Set(1, 2, 3), messageCustomizer _) must throwA[NoNodesAvailableException]
        networkClient.sendMessage(Set(1, 2, 3), messageCustomizer _) must throwA[NoNodesAvailableException]

        there were two(networkClient.lb).nextNode(1, None, None)

        // Test cases for fanout calls.
        networkClient.sendRequest(Set(1, 2, 3), 0, messageCustomizer _) must throwA[NoNodesAvailableException]
        networkClient.sendRequest(Set(1, 2, 3), 1, messageCustomizer _) must throwA[NoNodesAvailableException]
//      clusterIoClient.sendRequest(node, message, null) wasnt called
      }
    }

    "when sendRequest(ids, message, messageCustomizer, maxRetry) is called" in {

      val MAX_RETRY = 3

      "send the provided message to the node specified by the load balancer" in {
        clusterClient.nodes returns nodeSet
        clusterClient.isConnected returns true
        networkClient.clusterIoClient.nodesChanged(nodeSet) returns endpoints
        networkClient.loadBalancerFactory.newLoadBalancer(endpoints) returns networkClient.loadbalancer
        List(1, 2, 3).foreach(networkClient.lb.nextNode(_, None, None) returns Some(Node(1, "localhost:31313", true)))

        networkClient.start
        networkClient.sendRequest(Set(1, 2, 3), messageCustomizer _, MAX_RETRY)

        List(1, 2, 3).foreach(there was one(networkClient.lb).nextNode(_, None, None))
      }

      "call the message customizer" in {
        var callCount = 0
        var nodeMap = Map[Node, Set[Int]]()

        def mc(node: Node, ids: Set[Int]): Ping = {
          callCount += 1
          nodeMap = nodeMap + (node -> ids)
          new Ping
        }

        clusterClient.nodes returns nodeSet
        clusterClient.isConnected returns true
        networkClient.clusterIoClient.nodesChanged(nodeSet) returns endpoints
        networkClient.loadBalancerFactory.newLoadBalancer(endpoints) returns networkClient.loadbalancer
        List(1, 2).foreach(networkClient.lb.nextNode(_, None, None) returns Some(nodes(0)))
        List(3, 4).foreach(networkClient.lb.nextNode(_, None, None) returns Some(nodes(1)))

        networkClient.start
        networkClient.sendRequest(Set(1, 2, 3, 4), mc _, MAX_RETRY)

        callCount must be_==(2)
        nodeMap.size must be_==(2)
        nodeMap(nodes(0)) must haveTheSameElementsAs(Array(1, 2))
        nodeMap(nodes(1)) must haveTheSameElementsAs(Array(3, 4))
      }

      "treats an exception from the message customizer as a failed response" in {
        def mc(node: Node, ids: Set[Int]): Ping = {
          throw new Exception
        }

        clusterClient.nodes returns nodeSet
        clusterClient.isConnected returns true
        networkClient.clusterIoClient.nodesChanged(nodeSet) returns endpoints
        networkClient.loadBalancerFactory.newLoadBalancer(endpoints) returns networkClient.loadbalancer
        List(1, 2).foreach(networkClient.lb.nextNode(_, None, None) returns Some(nodes(0)))

        networkClient.start
        val ri = networkClient.sendRequest(Set(1, 2), mc _, MAX_RETRY)
        ri.hasNext must beTrue
        ri.next must throwA[ExecutionException]

        networkClient.sendRequest(Set(1, 2), 0, mc _, MAX_RETRY).hasNext must beTrue
        networkClient.sendRequest(Set(1, 2), 0, mc _, MAX_RETRY).next must throwA[ExecutionException]

        networkClient.sendRequest(Set(1, 2), 1, mc _, MAX_RETRY).hasNext must beTrue
        networkClient.sendRequest(Set(1, 2), 1, mc _, MAX_RETRY).next must throwA[ExecutionException]
      }

      "throw InvalidClusterException if there is no load balancer instance when sendRequest is called" in {
        clusterClient.nodes returns nodeSet
        clusterClient.isConnected returns true
        networkClient.clusterIoClient.nodesChanged(nodeSet) returns endpoints
        networkClient.loadBalancerFactory.newLoadBalancer(endpoints) throws new InvalidClusterException("")

        networkClient.start
        networkClient.sendRequest(Set(1, 2, 3), messageCustomizer _, MAX_RETRY)  must throwA[InvalidClusterException]

        networkClient.sendRequest(Set(1, 2, 3), 0, messageCustomizer _, MAX_RETRY)  must throwA[InvalidClusterException]
        networkClient.sendRequest(Set(1, 2, 3), 1, messageCustomizer _, MAX_RETRY)  must throwA[InvalidClusterException]
      }

      "throw NoSuchNodeException if load balancer returns None when sendRequest is called" in {
        clusterClient.nodes returns nodeSet
        clusterClient.isConnected returns true
        networkClient.clusterIoClient.nodesChanged(nodeSet) returns endpoints
        networkClient.loadBalancerFactory.newLoadBalancer(endpoints) returns networkClient.loadbalancer
        networkClient.lb.nextNode(1, None, None) returns None

        networkClient.start
        networkClient.sendRequest(Set(1, 2, 3), messageCustomizer _, MAX_RETRY) must throwA[NoNodesAvailableException]

        there was one(networkClient.lb).nextNode(1, None, None)

        networkClient.sendRequest(Set(1, 2, 3), 0, messageCustomizer _) must throwA[NoNodesAvailableException]
        networkClient.sendRequest(Set(1, 2, 3), 1, messageCustomizer _) must throwA[NoNodesAvailableException]
      }
    }


    "when sendRequestToReplicas(id, message) is called" in {
      "send the provided message to every replica specified by the load balancer" in {
        clusterClient.nodes returns nodeSet
        clusterClient.isConnected returns true
        networkClient.clusterIoClient.nodesChanged(nodeSet) returns endpoints
        networkClient.loadBalancerFactory.newLoadBalancer(endpoints) returns networkClient.loadbalancer
        List(1,2,3).foreach(networkClient.lb.nodesForPartitionedId(_, None, None) returns nodeSet)

        networkClient.start
        networkClient.sendRequestToReplicas(2, request)
        got {
          one(networkClient.lb).nodesForPartitionedId(2, None, None)
        }
      }

      "send the provided message to every replica specified by the load balancer filtering capability" in {
        clusterClient.nodes returns nodeSet
        clusterClient.isConnected returns true
        networkClient.clusterIoClient.nodesChanged(nodeSet) returns endpoints
        networkClient.loadBalancerFactory.newLoadBalancer(endpoints) returns networkClient.loadbalancer
        List(1,2,3).foreach(networkClient.lb.nodesForPartitionedId(_, Some(0xfL), Some(2L)) returns nodeSet)

        networkClient.start
        networkClient.sendRequestToReplicas(2, request, 0, Some(0xfL), Some(2L))
        got {
              one(networkClient.lb).nodesForPartitionedId(2, Some(0xfL), Some(2L))
            }
      }

      "throw InvalidClusterException if there is no loadbalancer instance when sendRequestToReplicas is called" in {
        clusterClient.nodes returns nodeSet
        clusterClient.isConnected returns true
        networkClient.clusterIoClient.nodesChanged(nodeSet) returns endpoints
        networkClient.loadBalancerFactory.newLoadBalancer(endpoints) throws new InvalidClusterException("")

        networkClient.start
        networkClient.sendRequestToReplicas(3, request) must throwA[InvalidClusterException]
      }

      "throw NoSuchNodeException if load balancer returns None when sendRequests is called" in {
        clusterClient.nodes returns nodeSet
        clusterClient.isConnected returns true
        networkClient.clusterIoClient.nodesChanged(nodeSet) returns endpoints
        networkClient.loadBalancerFactory.newLoadBalancer(endpoints) returns networkClient.loadbalancer
        networkClient.lb.nodesForPartitionedId(1, None, None) returns (Set.empty[Node])

        networkClient.start
        networkClient.sendRequestToReplicas(1, request) must throwA[NoNodesAvailableException]
        there was one(networkClient.lb).nodesForPartitionedId(1, None, None)
      }
      
    }

    "retryCallback should propagate server exception to underlying when" in {

      val MAX_RETRY = 3
      var either: Either[Throwable, Ping] = null
      val callback = (e: Either[Throwable, Ping]) => either = e

      "exception does not provide RequestAccess" in {
        networkClient.retryCallback[Ping, Ping](callback, 0, None, None)(Left(new Exception)) // fallback to underlying
        either must notBeNull
        either.isLeft must beTrue
      }

      "request.retryAttempt >= maxRetry" in {
        val req: Request[Ping, Ping] = spy(PartitionedRequest[Int, Ping, Ping](null, null, null, null, null, null, Some(callback), MAX_RETRY))
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
        networkClient.loadBalancerFactory.newLoadBalancer(endpoints) returns networkClient.loadbalancer
        networkClient.lb.nextNode(1, None, None) returns None
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
        networkClient.loadBalancerFactory.newLoadBalancer(endpoints) returns networkClient.loadbalancer
        networkClient.lb.nextNode(1, None, None) returns Some(nodes(1)) // node(1) -> node(1) -> node(1)

        networkClient.start

        var req: Request[Ping, Ping] = spy(PartitionedRequest[Int, Ping, Ping](null, nodes(1), null, null, null, null, Some(callback), 0))
        val ra: Exception with RequestAccess[Request[Ping, Ping]] = new Exception with RequestAccess[Request[Ping, Ping]] {
          def request = req
        }

        networkClient.retryCallback[Ping, Ping](callback, MAX_RETRY, None, None)(Left(ra))
        either must notBeNull
        either.isLeft must beTrue
        either.left.get mustEq ra
      }

      "sendMessage: MAX_RETRY reached" in {
        val nc2 = new PartitionedNetworkClient[Int] with ClusterClientComponent with ClusterIoClientComponent with PartitionedLoadBalancerFactoryComponent[Int] {
          val lb = new PartitionedLoadBalancer[Int] {
            var iter = PartitionedNetworkClientSpec.this.nodes.iterator
            def nextNode(id: Int, c: Option[Long] = None, pc : Option[Long] = None) = {
              if (!iter.hasNext ) iter = PartitionedNetworkClientSpec.this.nodes.iterator
              Some(iter.next)
            }
            def nodesForOneReplica(id: Int, c: Option[Long] = None, pc: Option[Long] = None) = null
            def nodesForPartitionedId(id:Int, c: Option[Long] = None, pc: Option[Long] = None) = null
            def nodesForPartitions(id: Int, partitions: Set[Int], c: Option[Long] = None, pc: Option[Long] = None) = null
          }
          val loadBalancerFactory = mock[PartitionedLoadBalancerFactory[Int]]
          val clusterIoClient = new ClusterIoClient {
            var invocationCount: Int = 0
            def sendMessage[RequestMsg, ResponseMsg](node: Node, requestCtx: Request[RequestMsg, ResponseMsg]) {
              invocationCount += 1
              requestCtx.onFailure(new Exception with RequestAccess[Request[RequestMsg, ResponseMsg]] {
                def request = requestCtx
              })
            }
            def nodesChanged(nodes: Set[Node]) = {PartitionedNetworkClientSpec.this.endpoints}
            def shutdown {}
          }
          val clusterClient = PartitionedNetworkClientSpec.this.clusterClient
        }
        nc2.clusterClient.nodes returns nodeSet
        nc2.clusterClient.isConnected returns true
        nc2.loadBalancerFactory.newLoadBalancer(endpoints) returns nc2.lb
        nc2.start
        val resIter = nc2.sendRequest(Set(1,2,3), messageCustomizer _, MAX_RETRY)
        nc2.clusterIoClient.invocationCount mustEqual (MAX_RETRY * 4)
        while (resIter.hasNext) {
          resIter.next must throwAnException[Exception]
        }
        val resIter2 = nc2.sendRequest(Set(1,2,3), 1, messageCustomizer _, MAX_RETRY)
        nc2.clusterIoClient.invocationCount mustEqual (MAX_RETRY * 8)
        while (resIter2.hasNext) {
          resIter2.next must throwAnException[Exception]
        }
      }

    }

    "sendMessage should automatically handle partial failures" in {
      val MAX_RETRY = 3
      val nc2 = new PartitionedNetworkClient[Int] with ClusterClientComponent with ClusterIoClientComponent with PartitionedLoadBalancerFactoryComponent[Int] {
        val lb = new PartitionedLoadBalancer[Int] {
          val nodeIS = PartitionedNetworkClientSpec.this.nodes.toIndexedSeq
          var idx = 0
          def nextNode(id: Int, c: Option[Long] = None, pc: Option[Long] = None) = {
            idx = (idx + 1) % nodeIS.size
            Some(nodeIS(idx))
          }
          def nodesForOneReplica(id: Int, c: Option[Long] = None, pc: Option[Long] = None) = null
          def nodesForPartitionedId(id:Int, c: Option[Long] = None, pc: Option[Long] = None) = null
          def nodesForPartitions(id: Int, partitions: Set[Int], c: Option[Long] = None, pc: Option[Long] = None)= null
        }
        val loadBalancerFactory = mock[PartitionedLoadBalancerFactory[Int]]
        val clusterIoClient = new ClusterIoClient {
          var succ: Boolean = false
          def sendMessage[RequestMsg, ResponseMsg](node: Node, requestCtx: Request[RequestMsg, ResponseMsg]) {
            if (!succ) {
              succ = true
              requestCtx.onFailure(new RemoteException("FooBar", "ServerError") with RequestAccess[Request[RequestMsg, ResponseMsg]] {
                def request = requestCtx
              })
            } else {
              succ = false
              requestCtx.onSuccess(requestCtx.outputSerializer.requestToBytes(requestCtx.message))
            }
          }
          def nodesChanged(nodes: Set[Node]) = {PartitionedNetworkClientSpec.this.endpoints}
          def shutdown {}
        }
        val clusterClient = PartitionedNetworkClientSpec.this.clusterClient
      }
      nc2.clusterClient.nodes returns nodeSet
      nc2.clusterClient.isConnected returns true
      nc2.loadBalancerFactory.newLoadBalancer(endpoints) returns nc2.lb
      nc2.start
      val resIter = nc2.sendRequest[Ping, Ping](Set(1,2), messageCustomizer _, MAX_RETRY)
      while (resIter.hasNext) {
        resIter.next mustNot throwAnException
      }
    }

    "sendMessage should automatically handle partial failures and adjust response size dynamically" in {
      val MAX_RETRY = 3
      val nc2 = new PartitionedNetworkClient[Int] with ClusterClientComponent with ClusterIoClientComponent with PartitionedLoadBalancerFactoryComponent[Int] {
        val lb = new PartitionedLoadBalancer[Int] {
          val nodeIS = PartitionedNetworkClientSpec.this.nodes.toIndexedSeq
          var count = 0
          def nextNode(id: Int, c: Option[Long] = None, pc: Option[Long] = None) = {
            var ret: Node = null
            if (count < 2)
              ret = nodes(0)
            else
              ret = nodes(id + 1)
            count += 1
            Some(ret)
          }
          def nodesForOneReplica(id: Int, c: Option[Long] = None, pc: Option[Long] = None) = null
          def nodesForPartitionedId(id:Int, c: Option[Long] = None, pc: Option[Long] = None) = null
          def nodesForPartitions(id: Int, partitions: Set[Int], c: Option[Long] = None, pc: Option[Long] = None) = null
        }
        val loadBalancerFactory = mock[PartitionedLoadBalancerFactory[Int]]
        val clusterIoClient = new ClusterIoClient {
          var failOnce: Boolean = true
          def sendMessage[RequestMsg, ResponseMsg](node: Node, requestCtx: Request[RequestMsg, ResponseMsg]) {
            if (failOnce) {
              failOnce = false
              requestCtx.onFailure(new RemoteException("FooBar", "ServerError") with RequestAccess[Request[RequestMsg, ResponseMsg]] {
                def request = requestCtx
              })
            } else {
              requestCtx.onSuccess(requestCtx.outputSerializer.requestToBytes(requestCtx.message))
            }
          }
          def nodesChanged(nodes: Set[Node]) = {PartitionedNetworkClientSpec.this.endpoints}
          def shutdown {}
        }
        val clusterClient = PartitionedNetworkClientSpec.this.clusterClient
      }
      nc2.clusterClient.nodes returns nodeSet
      nc2.clusterClient.isConnected returns true
      nc2.loadBalancerFactory.newLoadBalancer(endpoints) returns nc2.lb
      nc2.start
      val resIter = nc2.sendRequest[Ping, Ping](Set(0,1), messageCustomizer _, MAX_RETRY)
      var num = 0
      while (resIter.hasNext) {
        num += 1
        resIter.next mustNot throwAnException
      }
      num mustEq 2

      val resIter2 = nc2.sendRequest[Ping, Ping](Set(0,1), messageCustomizer _, MAX_RETRY)
      num = 0
      while (resIter2.hasNext) {
        num += 1
        resIter2.next mustNot throwAnException
      }
      num mustEq 2
    }


    "calculateNodesFromIds should properly exclude failing node" in {
      val nc2 = new PartitionedNetworkClient[Int] with ClusterClientComponent with ClusterIoClientComponent with PartitionedLoadBalancerFactoryComponent[Int] {
        val lb = new PartitionedLoadBalancer[Int] {
          val iter = PartitionedNetworkClientSpec.this.nodes.iterator
          def nextNode(id: Int, c: Option[Long] = None, pc: Option[Long] = None) = Some(iter.next)
          def nodesForOneReplica(id: Int, c: Option[Long] = None, pc: Option[Long] = None) = null
          def nodesForPartitionedId(id:Int, c: Option[Long] = None, pc: Option[Long] = None) = null
          def nodesForPartitions(id: Int, partitions: Set[Int], c: Option[Long] = None, pc: Option[Long] = None) = null
        }
        val loadBalancerFactory = mock[PartitionedLoadBalancerFactory[Int]]
        val clusterIoClient = new ClusterIoClient {
          var invocationCount: Int = 0
          def sendMessage[RequestMsg, ResponseMsg](node: Node, requestCtx: Request[RequestMsg, ResponseMsg]) {
            invocationCount += 1
            requestCtx.onFailure(new Exception with RequestAccess[Request[RequestMsg, ResponseMsg]] {
              def request = requestCtx
            })
          }
          def nodesChanged(nodes: Set[Node]) = {PartitionedNetworkClientSpec.this.endpoints}
          def shutdown {}
        }
        val clusterClient = PartitionedNetworkClientSpec.this.clusterClient
      }
      nc2.clusterClient.nodes returns nodeSet
      nc2.clusterClient.isConnected returns true
      nc2.loadBalancerFactory.newLoadBalancer(endpoints) returns nc2.lb
      nc2.start

      val failingNode = nodes(0)
      val failingNodes = Set(failingNode)
      var nodes2Ids = nc2.calculateNodesFromIds(Set(1), failingNodes, 3, None, None)
      nodes2Ids must notBeNull
      nodes2Ids.keys must notHave(failingNodes)
    }

    "calculateNodesFromIds should properly exclude failing nodes in excluded set" in {
      val nc2 = new PartitionedNetworkClient[Int] with ClusterClientComponent with ClusterIoClientComponent with PartitionedLoadBalancerFactoryComponent[Int] {
        val lb = new PartitionedLoadBalancer[Int] {
          val iter = PartitionedNetworkClientSpec.this.nodes.iterator
          def nextNode(id: Int, c: Option[Long] = None, pc: Option[Long] = None) = Some(iter.next)
          def nodesForOneReplica(id: Int, c: Option[Long] = None, pc: Option[Long] = None) = null
          def nodesForPartitionedId(id:Int, c: Option[Long] = None, pc: Option[Long] = None) = null
          def nodesForPartitions(id: Int, partitions: Set[Int], c: Option[Long] = None, pc: Option[Long] = None) = null
        }
        val loadBalancerFactory = mock[PartitionedLoadBalancerFactory[Int]]
        val clusterIoClient = new ClusterIoClient {
          var invocationCount: Int = 0
          def sendMessage[RequestMsg, ResponseMsg](node: Node, requestCtx: Request[RequestMsg, ResponseMsg]) {
            invocationCount += 1
            requestCtx.onFailure(new Exception with RequestAccess[Request[RequestMsg, ResponseMsg]] {
              def request = requestCtx
            })
          }
          def nodesChanged(nodes: Set[Node]) = {PartitionedNetworkClientSpec.this.endpoints}
          def shutdown {}
        }
        val clusterClient = PartitionedNetworkClientSpec.this.clusterClient
      }
      nc2.clusterClient.nodes returns nodeSet
      nc2.clusterClient.isConnected returns true
      nc2.loadBalancerFactory.newLoadBalancer(endpoints) returns nc2.lb
      nc2.start

      val failingNodes = Set(nodes(0), nodes(1))
      var nodes2Ids = nc2.calculateNodesFromIds(Set(1), failingNodes, 3, None, None)
      nodes2Ids must notBeNull
      nodes2Ids.keys must notHave(failingNodes)
    }

    "calculateNodesFromIds should throw NoNodesAvailableException if non-failing nodes not found" in {
      val nc2 = new PartitionedNetworkClient[Int] with ClusterClientComponent with ClusterIoClientComponent with PartitionedLoadBalancerFactoryComponent[Int] {
        val lb = new PartitionedLoadBalancer[Int] {
          val iter = PartitionedNetworkClientSpec.this.nodes.iterator
          def nextNode(id: Int, c: Option[Long] = None, pc: Option[Long] = None) = if (iter.hasNext) Some(iter.next) else None
          def nodesForOneReplica(id: Int, c: Option[Long] = None, pc: Option[Long] = None) = null
          def nodesForPartitionedId(id:Int, c: Option[Long] = None, pc: Option[Long] = None) = null
          def nodesForPartitions(id: Int, partitions: Set[Int], c: Option[Long] = None, pc: Option[Long] = None) = null
        }
        val loadBalancerFactory = mock[PartitionedLoadBalancerFactory[Int]]
        val clusterIoClient = new ClusterIoClient {
          var invocationCount: Int = 0
          def sendMessage[RequestMsg, ResponseMsg](node: Node, requestCtx: Request[RequestMsg, ResponseMsg]) {
            invocationCount += 1
            requestCtx.onFailure(new Exception with RequestAccess[Request[RequestMsg, ResponseMsg]] {
              def request = requestCtx
            })
          }
          def nodesChanged(nodes: Set[Node]) = {PartitionedNetworkClientSpec.this.endpoints}
          def shutdown {}
        }
        val clusterClient = PartitionedNetworkClientSpec.this.clusterClient
      }
      nc2.clusterClient.nodes returns nodeSet
      nc2.clusterClient.isConnected returns true
      nc2.loadBalancerFactory.newLoadBalancer(endpoints) returns nc2.lb
      nc2.start

      val failingNodes = Set(nodes(0), nodes(1), nodes(2))
      nc2.calculateNodesFromIds(Set(1), failingNodes, 3, None, None) must throwA[NoNodesAvailableException]
    }

    "when sendRequest is called with a response aggregator" in {
      "it calls the response aggregator" in {
        var callCount = 0
        def ag(ri: ResponseIterator[Ping]) = {
          callCount += 1
          123454321
        }

        clusterClient.nodes returns nodeSet
        clusterClient.isConnected returns true
        networkClient.clusterIoClient.nodesChanged(nodeSet) returns endpoints
        networkClient.loadBalancerFactory.newLoadBalancer(endpoints) returns networkClient.loadbalancer
        List(1, 2).foreach(networkClient.lb.nextNode(_, None, None) returns Some(nodes(0)))

        networkClient.start
        networkClient.sendRequest(Set(1, 2), (node: Node, ids: Set[Int]) => request, ag _) must be_==(123454321)

        callCount must be_==(1)

        networkClient.sendRequest(Set(1, 2), 0, (node: Node, ids: Set[Int]) => request, ag _) must be_==(123454321)

        callCount must be_==(2)

        networkClient.sendRequest(Set(1, 2), 1, (node: Node, ids: Set[Int]) => request, ag _) must be_==(123454321)

        callCount must be_==(3)
      }

      "it rethrows exceptions thrown by the response aggregator" in {
        def ag(ri: ResponseIterator[Ping]): Int = {
          throw new Exception
        }

        clusterClient.nodes returns nodeSet
        clusterClient.isConnected returns true
        networkClient.clusterIoClient.nodesChanged(nodeSet) returns endpoints
        networkClient.loadBalancerFactory.newLoadBalancer(endpoints) returns networkClient.loadbalancer
        List(1, 2).foreach(networkClient.lb.nextNode(_, None, None) returns Some(nodes(0)))

        networkClient.start
        networkClient.sendRequest(Set(1, 2), (node: Node, ids: Set[Int]) => request, ag _) must throwA[Exception]

        networkClient.sendRequest(Set(1, 2), 0, (node: Node, ids: Set[Int]) => request, ag _) must throwA[Exception]
        networkClient.sendRequest(Set(1, 2), 1, (node: Node, ids: Set[Int]) => request, ag _) must throwA[Exception]
      }

      "Automatically fix partitioned requests " in {
        val testData = Map(
          Node(id = 0, url = "", available = true, partitionIds = Set(1, 2, 3)) -> Set(1, 2),
          Node(id = 1, url = "", available = true, partitionIds = Set(1, 2, 3)) -> Set(2, 3))

        val corrected = networkClient.ensureReplicaConsistency(testData)

        // Search for duplicates
        corrected.size must be_==(2)
        testData.values.map(_.toSeq).flatten.size must be_==(4)
        corrected.values.map(_.toSeq).flatten.size must be_==(3)
      }
    }
  }

  def messageCustomizer(node: Node, ids: Set[Int]): Ping = new Ping
}