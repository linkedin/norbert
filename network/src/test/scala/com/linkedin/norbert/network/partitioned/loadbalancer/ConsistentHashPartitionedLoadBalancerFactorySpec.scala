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
package loadbalancer

import com.linkedin.norbert.cluster.{InvalidClusterException, Node}
import com.linkedin.norbert.network.common.Endpoint
import org.specs2.mutable.SpecificationWithJUnit
import org.specs2.specification.Scope

class ConsistentHashPartitionedLoadBalancerFactorySpec extends SpecificationWithJUnit {

  trait ConsistentHashPartitionedLoadBalancerSetup extends Scope {

    case class EId(id: Int)

    implicit def eId2ByteArray(eId: EId): Array[Byte] = BigInt(eId.id).toByteArray

    class EIdDefaultLoadBalancerFactory(numPartitions: Int, serveRequestsIfPartitionMissing: Boolean) extends DefaultPartitionedLoadBalancerFactory[EId](numPartitions, serveRequestsIfPartitionMissing) {
      protected def calculateHash(id: EId) = HashFunctions.fnv(id)

      def getNumPartitions(endpoints: Set[Endpoint]) = numPartitions
    }

    def toEndpoints(nodes: Set[Node], failingNodes: Set[Node] = Set.empty[Node]) = nodes.map(n => new Endpoint {
      def node = n

      def canServeRequests = !failingNodes.contains(n)
    })

    val loadBalancerFactory = new EIdDefaultLoadBalancerFactory(5, true)
  }

  "ConsistentHashPartitionedLoadBalancer" should {
    "nextNode returns the correct node for 1210" in new ConsistentHashPartitionedLoadBalancerSetup {
      val nodes = Set(
        Node(0, "localhost:31313", true, Set(0, 1)),
        Node(1, "localhost:31313", true, Set(1, 2)),
        Node(2, "localhost:31313", true, Set(2, 3)),
        Node(3, "localhost:31313", true, Set(3, 4)),
        Node(4, "localhost:31313", true, Set(0, 4)))

      val lb = loadBalancerFactory.newLoadBalancer(toEndpoints(nodes))
      lb.nextNode(EId(1210), Some(0L), Some(0L)) must beSome[Node].which(List(Node(0, "localhost:31313", true, Set(0, 1)),
        Node(4, "localhost:31313", true, Set(0, 4))) must contain(_))
    }

    "nextNode with capability return the correct node for 1210" in new ConsistentHashPartitionedLoadBalancerSetup {
      val nodes = Set(
        Node(0, "localhost:31313", true, Set(0, 1)),
        Node(1, "localhost:31313", true, Set(1, 2)),
        Node(2, "localhost:31313", true, Set(2, 3)),
        Node(3, "localhost:31313", true, Set(3, 4)),
        Node(4, "localhost:31313", true, Set(0, 4), Some(0x2), Some(2)),
        Node(5, "localhost:31313", true, Set(3, 0), Some(0x3), Some(2)))

      val lb = loadBalancerFactory.newLoadBalancer(toEndpoints(nodes))
      lb.nextNode(EId(1210), Some(0L)) must beSome[Node].which(List(Node(0, "localhost:31313", true, Set(0, 1)),
        Node(4, "localhost:31313", true, Set(0, 4), Some(0x2), Some(2)),
        Node(5, "localhost:31313", true, Set(3, 0), Some(0x3), Some(2))) must contain(_))
      lb.nextNode(EId(1210), Some(0x1)) must be_==(Some(Node(5, "localhost:31313", true, Set(3, 0), Some(0x3), Some(2))))
      lb.nextNode(EId(1210), Some(0x2)) must beSome[Node].which(List(Node(4, "localhost:31313", true, Set(0, 4), Some(0x2), Some(2)),
        Node(5, "localhost:31313", true, Set(3, 0), Some(0x3), Some(2))) must contain(_))
      //HIGH TODO: overflow @DefaultLoadBalancerHelper.nodeForPartition
      //lb.nextNode(EId(1210), Some(0x4)) must be_==(None)
    }


    "throw InvalidClusterException if all partitions are unavailable" in new ConsistentHashPartitionedLoadBalancerSetup {
      val nodes = Set(
        Node(0, "localhost:31313", true, Set[Int]()),
        Node(1, "localhost:31313", true, Set[Int]()))

      new EIdDefaultLoadBalancerFactory(2, false).newLoadBalancer(toEndpoints(nodes)) must throwA[InvalidClusterException]
    }

    "throw InvalidClusterException if one partition is unavailable, and the LBF cannot serve requests in that state, " in new ConsistentHashPartitionedLoadBalancerSetup {
      val nodes = Set(
        Node(0, "localhost:31313", true, Set(1)),
        Node(1, "localhost:31313", true, Set[Int]()))

      new EIdDefaultLoadBalancerFactory(2, true).newLoadBalancer(toEndpoints(nodes)) must not(throwA[InvalidClusterException])
      new EIdDefaultLoadBalancerFactory(2, false).newLoadBalancer(toEndpoints(nodes)) must throwA[InvalidClusterException]
    }

    "nodesForPartitionedId returns all the correct nodes for 1210" in new ConsistentHashPartitionedLoadBalancerSetup {
      val nodes = Set(
        Node(0, "localhost:12345", true, Set(0, 1, 2)),
        Node(1, "localhost:23451", true, Set(1, 2, 3)),
        Node(2, "localhost:34512", true, Set(2, 3, 4)),
        Node(3, "localhost:45123", true, Set(3, 4, 0)),
        Node(4, "localhost:51234", true, Set(4, 0, 1))
      )
      val lb = loadBalancerFactory.newLoadBalancer(toEndpoints(nodes))
      lb.nodesForPartitionedId(EId(1210), Some(0L), Some(0L)) mustEqual (Set(Node(0, "localhost:12345", true, Set(0, 1, 2)),
        Node(3, "localhost:45123", true, Set(3, 4, 0)),
        Node(4, "localhost:51234", true, Set(4, 0, 1))))
    }

    "nodesForPartitionedId returns all nodes regardless if they can serve requests" in new ConsistentHashPartitionedLoadBalancerSetup {
      val nodes = Set(
        Node(0, "localhost:12345", true, Set(0, 1, 2)),
        Node(1, "localhost:23451", true, Set(1, 2, 3)),
        Node(2, "localhost:34512", true, Set(2, 3, 4)),
        Node(3, "localhost:45123", true, Set(3, 4, 0)),
        Node(4, "localhost:51234", true, Set(4, 0, 1))
      )
      val lb = loadBalancerFactory.newLoadBalancer(toEndpoints(nodes, Set(nodes.last)))
      lb.nodesForPartitionedId(EId(1210), Some(0L), Some(0L)) mustEqual (Set(Node(0, "localhost:12345", true, Set(0, 1, 2)),
        Node(3, "localhost:45123", true, Set(3, 4, 0)),
        Node(4, "localhost:51234", true, Set(4, 0, 1))))
    }

    "nodesForPartitionedId returns only nodes satisfying capabiity requirements" in new ConsistentHashPartitionedLoadBalancerSetup {
      val nodes = Set(
        Node(0, "localhost:12345", true, Set(0, 1, 2), Some(0x1), Some(0)),
        Node(1, "localhost:23451", true, Set(1, 2, 3)),
        Node(2, "localhost:34512", true, Set(2, 3, 4)),
        Node(3, "localhost:45123", true, Set(3, 4, 0)),
        Node(4, "localhost:51234", true, Set(4, 0, 1), Some(0x2), Some(0))
      )
      val lb = loadBalancerFactory.newLoadBalancer(toEndpoints(nodes))
      lb.nodesForPartitionedId(EId(1210), Some(0L), Some(0L)) mustEqual (Set(Node(0, "localhost:12345", true, Set(0, 1, 2)),
        Node(3, "localhost:45123", true, Set(3, 4, 0)),
        Node(4, "localhost:51234", true, Set(4, 0, 1))))

      lb.nodesForPartitionedId(EId(1210), Some(0x1), Some(0L)) mustEqual (Set(Node(0, "localhost:12345", true, Set(0, 1, 2), Some(0x1), Some(0))))
      lb.nodesForPartitionedId(EId(1210), Some(0x2), Some(0L)) mustEqual (Set(Node(4, "localhost:51234", true, Set(4, 0, 1), Some(0x2), Some(0))))
      lb.nodesForPartitionedId(EId(1210), Some(0x3), Some(0L)) mustEqual (Set.empty[Node])
    }

    "nodesForOneReplica returns only nodes satisfying capability requirements" in new ConsistentHashPartitionedLoadBalancerSetup {
      val nodes = Set(
        Node(0, "localhost:12345", true, Set(0, 1, 2), Some(0x1), Some(2)),
        Node(1, "localhost:23451", true, Set(1, 2, 3)),
        Node(2, "localhost:34512", true, Set(2, 3, 4), Some(0x2), Some(2)),
        Node(3, "localhost:45123", true, Set(3, 4, 0), Some(0x3), Some(2)),
        Node(4, "localhost:51234", true, Set(4, 0, 1), Some(0x6), Some(2))
      )
      val lb = loadBalancerFactory.newLoadBalancer(toEndpoints(nodes))
      lb.nodesForOneReplica(EId(1210), Some(0), Some(0)).flatten(_._2) mustEqual (Set(0, 1, 2, 3, 4))
      lb.nodesForOneReplica(EId(1210), Some(0x2), Some(2)).flatten(_._2) mustEqual (Set(0, 1, 2, 3, 4))
    }

    "DefaultPartitionedLoadBalancer can properly balance the load without capability" in new ConsistentHashPartitionedLoadBalancerSetup {
      val nodes = Set(
        Node(0, "localhost:12345", true, Set(0, 1, 2)),
        Node(1, "localhost:23451", true, Set(1, 2, 3)),
        Node(2, "localhost:34512", true, Set(2, 3, 4)),
        Node(3, "localhost:45123", true, Set(3, 4, 0)),
        Node(4, "localhost:51234", true, Set(4, 0, 1)),
        Node(5, "localhost:13245", true, Set(0, 1, 3))
      )
      val lb = loadBalancerFactory.newLoadBalancer(toEndpoints(nodes))
      val accessVector = Array(0, 0, 0, 0, 0, 0)
      (0 to 11).foreach { (i) =>
        val node: Node = lb.nextNode(EId(1210)).get
        if (!node.eq(None))
          accessVector(node.id) = accessVector(node.id) + 1
      }
      accessVector(0) must be_==(accessVector(3))
      accessVector(3) must be_==(accessVector(4))
      accessVector(4) must be_==(accessVector(5))
      accessVector(5) must be_==(3)
    }

    "DefaultPartitionedLoadBalancer can properly load balance among nodes satisfying capability" in new ConsistentHashPartitionedLoadBalancerSetup {
      val nodes = Set(
        Node(0, "localhost:12345", true, Set(0, 1, 2), Some(0x1L), Some(2L)),
        Node(1, "localhost:23451", true, Set(1, 2, 3)),
        Node(2, "localhost:34512", true, Set(2, 3, 4)),
        Node(3, "localhost:45123", true, Set(3, 4, 0), Some(0x1L), Some(2L)),
        Node(4, "localhost:51234", true, Set(4, 0, 1), Some(0x2L), Some(2L)),
        Node(5, "localhost:13245", true, Set(0, 1, 3), Some(0x2L), Some(2L))
      )
      val lb = loadBalancerFactory.newLoadBalancer(toEndpoints(nodes))
      val accessVector = Array(0, 0, 0, 0, 0, 0)
      (0 to 11).foreach { (i) =>
        val node1: Node = lb.nextNode(EId(1210), Some(0x1L), Some(2L)).get
        if (!node1.eq(None))
          accessVector(node1.id) = accessVector(node1.id) + 1
      }

      (0 to 11).foreach { (i) =>
        val node2: Node = lb.nextNode(EId(1210), Some(0x2L), Some(2L)).get
        if (!node2.eq(None))
          accessVector(node2.id) = accessVector(node2.id) + 1
      }

      accessVector(0) must be_==(accessVector(3))
      accessVector(3) must be_==(accessVector(4))
      accessVector(4) must be_==(accessVector(5))
      accessVector(5) must be_==(6)
    }
  }
}