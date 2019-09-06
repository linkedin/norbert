package com.linkedin.norbert.network.partitioned.loadbalancer

import com.linkedin.norbert.cluster.{InvalidClusterException, Node}
import com.linkedin.norbert.network.common.Endpoint
import org.specs2.mutable.SpecificationWithJUnit
import org.specs2.specification.Scope

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

class PartitionedConsistentHashedLoadBalancerSpec extends SpecificationWithJUnit {

  trait ConsistentHashPartitionedLoadBalancerSetup extends Scope {

    class TestLBF(numPartitions: Int, csr: Boolean = true)
      extends PartitionedConsistentHashedLoadBalancerFactory[Int](numPartitions,
        10,
        (id: Int) => HashFunctions.fnv(BigInt(id).toByteArray),
        (str: String) => str.hashCode(),
        csr)

    class TestEndpoint(val node: Node, var csr: Boolean) extends Endpoint {
      def canServeRequests = csr

      def setCsr(ncsr: Boolean) {
        csr = ncsr
      }
    }

    def toEndpoints(nodes: Set[Node]): Set[Endpoint] = nodes.map(n => new TestEndpoint(n, true))

    def markUnavailable(endpoints: Set[Endpoint], id: Int) {
      endpoints.foreach { endpoint =>
        if (endpoint.node.id == id) {
          endpoint.asInstanceOf[TestEndpoint].setCsr(false)
        }
      }
    }

    val loadBalancerFactory = new TestLBF(5)

    //  "DefaultPartitionedLoadBalancerFactory" should {
    //    "return the correct partition id" in {
    //      loadBalancerFactory.partitionForId(EId(1210)) must be_==(0)
    //    }
    //  }

    val sampleNodes = Set(
      Node(0, "localhost:31313", true, Set(0, 1), Some(0x1), Some(0)),
      Node(1, "localhost:31313", true, Set(1, 2)),
      Node(2, "localhost:31313", true, Set(2, 3)),
      Node(3, "localhost:31313", true, Set(3, 4)),
      Node(4, "localhost:31313", true, Set(0, 4), Some(0x2), Some(0)))
  }

  "ConsistentHashPartitionedLoadBalancer" should {
    "nextNode returns the correct node for 1210" in new ConsistentHashPartitionedLoadBalancerSetup {
      val nodes = sampleNodes
      val lb = loadBalancerFactory.newLoadBalancer(toEndpoints(nodes))
      lb.nextNode(1210) must beSome[Node].which(List(Node(0, "localhost:31313", true, Set(0, 1)),
        Node(4, "localhost:31313", true, Set(0, 4))) must contain(_))

      lb.nextNode(1210, Some(0x1)) must be_==(Some(Node(0, "localhost:31313", true, Set(0, 1))))
      lb.nextNode(1210, Some(0x2)) must be_==(Some(Node(4, "localhost:31313", true, Set(0, 4))))
      lb.nextNode(1210, Some(0x5)) must be_==(None)
    }

    "throw InvalidClusterException if all partitions are unavailable" in new ConsistentHashPartitionedLoadBalancerSetup {
      val nodes = Set(
        Node(0, "localhost:31313", true, Set[Int]()),
        Node(1, "localhost:31313", true, Set[Int]()))

      new TestLBF(2, false).newLoadBalancer(toEndpoints(nodes)) must throwA[InvalidClusterException]
    }

    "throw InvalidClusterException if one partition is unavailable, and the LBF cannot serve requests in that state, " in new ConsistentHashPartitionedLoadBalancerSetup {
      val nodes = Set(
        Node(0, "localhost:31313", true, Set(1)),
        Node(1, "localhost:31313", true, Set[Int]()))

      new TestLBF(2, true).newLoadBalancer(toEndpoints(nodes)) must not(throwA[InvalidClusterException])
      new TestLBF(2, false).newLoadBalancer(toEndpoints(nodes)) must throwA[InvalidClusterException]
    }

    "successfully calculate broadcast nodes" in new ConsistentHashPartitionedLoadBalancerSetup {
      val nodes = sampleNodes
      val loadBalancer = loadBalancerFactory.newLoadBalancer(toEndpoints(nodes))
      val replica1 = loadBalancer.nodesForOneReplica(0, Some(0), Some(0))
      replica1.values.flatten.toSet must be_==(Set(0, 1, 2, 3, 4))

      val replica2 = loadBalancer.nodesForOneReplica(1, Some(0), Some(0))
      replica2.values.flatten.toSet must be_==(Set(0, 1, 2, 3, 4))

      val replica3 = loadBalancer.nodesForOneReplica(2, Some(0), Some(0))
      replica3.values.flatten.toSet must be_==(Set(0, 1, 2, 3, 4))

      replica1.keySet mustEqual replica2.keySet
      replica1.keySet mustNotEqual replica3.keySet
      replica2.keySet mustNotEqual replica3.keySet
    }

    "nodesForPartitionedId returns the correct node for 1210" in new ConsistentHashPartitionedLoadBalancerSetup {
      val nodes = sampleNodes
      val lb = loadBalancerFactory.newLoadBalancer(toEndpoints(nodes))
      lb.nodesForPartitionedId(1210, Some(0L), Some(0L)) must containTheSameElementsAs(Seq(Node(0, "localhost:31313", true, Set(0, 1)), Node(4, "localhost:31313", true, Set(0, 4))))
      lb.nodesForPartitionedId(1210, Some(0x1), Some(0)) must containTheSameElementsAs(Seq(Node(0, "localhost:31313", true, Set(0, 1))))
    }

    "handle endpoints going down" in new ConsistentHashPartitionedLoadBalancerSetup {
      val nodes = sampleNodes
      val endpoints = toEndpoints(nodes)

      var loadBalancer = loadBalancerFactory.newLoadBalancer(endpoints)
      val replica1 = loadBalancer.nodesForOneReplica(0, Some(0), Some(0))
      replica1.values.flatten.toSet must be_==(Set(0, 1, 2, 3, 4))

      // Mark node 0 down
      markUnavailable(endpoints, 0)

      // Check we can still serve requests
      loadBalancer = loadBalancerFactory.newLoadBalancer(endpoints)
      val replica2 = loadBalancer.nodesForOneReplica(0, Some(0), Some(0))
      replica2.values.flatten.toSet must be_==(Set(0, 1, 2, 3, 4))

      // Mark node 4 down
      markUnavailable(endpoints, 4)

      // Check we can still serve requests
      loadBalancer = loadBalancerFactory.newLoadBalancer(endpoints)
      val replica3 = loadBalancer.nodesForOneReplica(0, Some(0), Some(0))
      replica3.values.flatten.toSet must be_==(Set(0, 1, 2, 3, 4))
    }

    "throw an exception if partitions are unavailable and we don't allow fault tolerance" in new ConsistentHashPartitionedLoadBalancerSetup {
      val nodes = sampleNodes
      val endpoints = toEndpoints(nodes)

      // Mark node 0 down
      markUnavailable(endpoints, 0)
      // Mark node 4 down
      markUnavailable(endpoints, 4)

      val lbf = new TestLBF(5, false)
      var loadBalancer = lbf.newLoadBalancer(endpoints)
      loadBalancer.nodesForOneReplica(0, Some(0), Some(0)) must throwA[InvalidClusterException]
    }
  }
}