package com.linkedin.norbert.network.partitioned.loadbalancer

import org.specs.SpecificationWithJUnit
import com.linkedin.norbert.network.common.Endpoint
import com.linkedin.norbert.cluster.{InvalidClusterException, Node}

import scala.collection.JavaConversions
import scala.collection.immutable.HashSet

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
  class TestLBF(numPartitions: Int, hashFunction:((Int) => Int), numReplicas:Int = 10, csr: Boolean = true)
          extends PartitionedConsistentHashedLoadBalancerFactory[Int](numPartitions,
            numReplicas,
            hashFunction,
            (str: String) => str.hashCode(),
            csr) {
    def this(numPartitions: Int, numReplicas:Int = 10, csr: Boolean = true) =
      this(numPartitions, (id: Int) => HashFunctions.fnv(BigInt(id).toByteArray), numReplicas, csr)
  }

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

  val overlappingAtPartitionZero = Set(
    Node(0, "localhost:31311", true, Set(0,1)),
    Node(1, "localhost:31312", true, Set(2)),
    Node(2, "localhost:31313", true, Set(0,3)),
    Node(3, "localhost:31314", true, Set(0,4)),
    Node(4, "localhost:31315", true, Set(4))
  )

  val sampleNodes = Set(
    Node(0, "localhost:31313", true, Set(0, 1), Some(0x1), Some(0)),
    Node(1, "localhost:31313", true, Set(1, 2)),
    Node(2, "localhost:31313", true, Set(2, 3)),
    Node(3, "localhost:31313", true, Set(3, 4)),
    Node(4, "localhost:31313", true, Set(0, 4), Some(0x2), Some(0)))
  

  "ConsistentHashPartitionedLoadBalancer" should {
    "nextNode returns the correct node for 1210" in {
      val nodes = sampleNodes
      val lb = loadBalancerFactory.newLoadBalancer(toEndpoints(nodes))
      lb.nextNode(1210) must beSome[Node].which(List(Node(0, "localhost:31313", true, Set(0, 1)),
        Node(4, "localhost:31313", true, Set(0, 4))) must contain(_))
      
      lb.nextNode(1210, Some(0x1)) must be_==(Some(Node(0, "localhost:31313", true, Set(0, 1))))
      lb.nextNode(1210, Some(0x2)) must be_==(Some(Node(4, "localhost:31313", true, Set(0, 4))))
      lb.nextNode(1210, Some(0x5)) must be_==(None)
    }

    "throw InvalidClusterException if all partitions are unavailable" in {
      val nodes = Set(
        Node(0, "localhost:31313", true, Set[Int]()),
        Node(1, "localhost:31313", true, Set[Int]()))

      new TestLBF(2,10, false).newLoadBalancer(toEndpoints(nodes)) must throwA[InvalidClusterException]
    }

    "throw InvalidClusterException if one partition is unavailable, and the LBF cannot serve requests in that state, " in {
      val nodes = Set(
        Node(0, "localhost:31313", true, Set(1)),
        Node(1, "localhost:31313", true, Set[Int]()))

      new TestLBF(2,10, true).newLoadBalancer(toEndpoints(nodes)) must not (throwA[InvalidClusterException])
      new TestLBF(2,10, false).newLoadBalancer(toEndpoints(nodes)) must throwA[InvalidClusterException]
    }
    
    "successfully calculate broadcast nodes" in {
      val nodes = sampleNodes
      val loadBalancer = loadBalancerFactory.newLoadBalancer(toEndpoints(nodes))
      val replica1 = loadBalancer.nodesForOneReplica(0, Some(0), Some(0))
      replica1.values.flatten.toSet must be_==(Set(0, 1, 2, 3, 4))

      val replica2 = loadBalancer.nodesForOneReplica(1, Some(0), Some(0))
      replica2.values.flatten.toSet must be_==(Set(0, 1, 2, 3, 4))

      replica1.keySet mustNotEq replica2.keySet
    }
    
    "nodesForPartitionedId returns the correct node for 1210" in {
      val nodes = sampleNodes
      val lb = loadBalancerFactory.newLoadBalancer(toEndpoints(nodes))
      lb.nodesForPartitionedId(1210, Some(0L), Some(0L)) must haveTheSameElementsAs (Set(Node(0, "localhost:31313", true, Set(0, 1)), Node(4, "localhost:31313", true, Set(0, 4))))
      lb.nodesForPartitionedId(1210, Some(0x1), Some(0)) must haveTheSameElementsAs (Set(Node(0, "localhost:31313", true, Set(0, 1))))
    }

    "handle endpoints going down" in {
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

    "throw an exception if partitions are unavailable and we don't allow fault tolerance" in {
      val nodes = sampleNodes
      val endpoints = toEndpoints(nodes)

      // Mark node 0 down
      markUnavailable(endpoints, 0)
      // Mark node 4 down
      markUnavailable(endpoints, 4)

      val lbf = new TestLBF(5, 10, false)
      var loadBalancer = lbf.newLoadBalancer(endpoints)
      loadBalancer.nodesForOneReplica(0, Some(0), Some(0)) must throwA[InvalidClusterException]
    }

    "return a complete set of nodes within partition 0" in {
      val nodes = overlappingAtPartitionZero
      val endpoints = toEndpoints(nodes)
      val lbf = new TestLBF(5, (i:Int)=>i, 10, true)
      val loadBalancer = lbf.newLoadBalancer(endpoints)
      val lbNodes = JavaConversions.asScalaSet(loadBalancer.nextNodes(0, None, None))
      val lbNodeIds = lbNodes.map((n:Node) => n.id)
      lbNodeIds must be_==(Set(0, 2, 3))
    }
  }

}