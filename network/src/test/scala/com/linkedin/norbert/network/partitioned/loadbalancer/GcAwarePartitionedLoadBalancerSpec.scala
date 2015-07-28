/*
 * Copyright 2009-2015 LinkedIn, Inc
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
package com.linkedin.norbert.network.partitioned.loadbalancer

import com.linkedin.norbert.cluster.{InvalidClusterException, Node}
import com.linkedin.norbert.network.NoNodesAvailableException
import com.linkedin.norbert.network.common.Endpoint
import com.linkedin.norbert.network.partitioned.loadbalancer.gcaware.GcAwarePartitionedLoadBalancerFactory
import org.specs.SpecificationWithJUnit
import org.specs.util.WaitFor


/**
 * @author: sishah
 * @date: 06/18/15
 * @version: 1.0
 */

class GcAwarePartitionedLoadBalancerSpec extends SpecificationWithJUnit with WaitFor {

  val cycleTime = 6000
  val slotTime = 2000

  // Buffer time, in milliseconds, to prevent test cases failing at slot transition boundaries.
  // i.e. we wait this amount of time into a particular slot before testing
  val slackTime = 10

  class TestLBF(numPartitions: Int, csr: Boolean = true)
        extends GcAwarePartitionedLoadBalancerFactory[Int](numPartitions, cycleTime, slotTime, csr)
  {
    protected def calculateHash(id: Int) = HashFunctions.fnv(BigInt(id).toByteArray)

    def getNumPartitions(endpoints: Set[Endpoint]) = numPartitions
  }

  val loadBalancerFactory = new TestLBF(6)

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

  val node1 = new Node(1, "node 1", true, Set(0,1,2), None, None, Some(0))
  val node2 = new Node(2, "node 2", true, Set(3,4,5), None, None, Some(0))
  val node3 = new Node(3, "node 3", true, Set(1,2,3), None, None, Some(1))
  val node4 = new Node(4, "node 4", true, Set(4,5,0), None, None, Some(1))
  val node5 = new Node(5, "node 5", true, Set(1,2,3), None, None, Some(2))
  val node6 = new Node(6, "node 6", true, Set(4,5,0), None, None, Some(2))

  val nodes = Set(node1, node2, node3, node4, node5, node6)

  "Set cover GC-aware load balancer" should {
    "nodesForPartitions returns nodes cover the input partitioned Ids" in {
      val loadbalancer = loadBalancerFactory.newLoadBalancer(toEndpoints(nodes))
      val res = loadbalancer.nodesForPartitionedIds(Set(0,1,3,4), Some(0L), Some(0L))
      res.values.flatten.toSet must be_==(Set(1, 0, 3, 4))
    }

    "nodesForPartitions returns partial results when not all partitions available" in {
      val endpoints = toEndpoints(nodes)
      markUnavailable(endpoints, 1)
      markUnavailable(endpoints, 3)
      markUnavailable(endpoints, 5)

      val loadbalancer = loadBalancerFactory.newLoadBalancer(endpoints)
      val res = loadbalancer.nodesForPartitionedIds(Set(3,5), Some(0L), Some(0L))
      res must be_== (Map())
    }

    "throw NoNodeAvailable if partition is missing and serveRequestsIfPartitionMissing set to false" in {
      val endpoints = toEndpoints(nodes)
      val loadbalancer = new TestLBF(6, false).newLoadBalancer(endpoints)
      val res = loadbalancer.nodesForPartitionedIds(Set(1,3,4,5), Some(0L), Some(0L))
      res.values.flatten.toSet must be_==(Set(1,3,4,5))

      markUnavailable(endpoints, 1)
      markUnavailable(endpoints, 3)
      markUnavailable(endpoints, 5)
      loadbalancer.nodesForPartitionedIds(Set(1,3,4,5), Some(0L), Some(0L)) must throwA[NoNodesAvailableException]
    }

    "not throw InvalidClusterException if a node doesn't have an offset" in {
      val badNode = new Node(2, "node 2", true, Set(3,4,5))

      val badNodeSet = Set(new Node(1, "node 1", true, Set(0,1,2), None, None, Some(0)),
        badNode,
        new Node(3, "node 3", true, Set(1,2,3), None, None, Some(1)),
        new Node(4, "node 4", true, Set(4,5,0), None, None, Some(1)))

      val endpoints = toEndpoints(badNodeSet)
      loadBalancerFactory.newLoadBalancer(endpoints) mustNot throwA[InvalidClusterException]
      val loadbalancer = loadBalancerFactory.newLoadBalancer(endpoints)

      while(System.currentTimeMillis()%cycleTime != 0){}
      waitFor((slotTime + slackTime).ms)

      val idCorrespondingToPartition4 = 1318
      val res = loadbalancer.nextNode(idCorrespondingToPartition4)
      res must beSome(badNode)

    }

    "not return a Node which is currently GCing" in {
      val loadbalancer = loadBalancerFactory.newLoadBalancer(toEndpoints(nodes))
      while(System.currentTimeMillis()%cycleTime != 0){}

      val idCorrespondingToPartition1 = 1210
      val idCorrespondingToPartition4 = 1318

      val possibleNodeSet = scala.collection.mutable.Set(node3,node5)
      val res = loadbalancer.nextNode(idCorrespondingToPartition1)
      res must beSome[Node].which(possibleNodeSet must contain(_))
      possibleNodeSet remove res.get
      val res2 = loadbalancer.nextNode(idCorrespondingToPartition1)
      res2 must beSome[Node].which(possibleNodeSet must contain(_))

      while(System.currentTimeMillis()%slotTime != 0){}
      val possibleNodeSet2 = scala.collection.mutable.Set(node2,node6)
      val res3 = loadbalancer.nextNode(idCorrespondingToPartition4)
      res3 must beSome[Node].which(possibleNodeSet2 must contain(_))
      possibleNodeSet2 remove res3.get
      val res4 = loadbalancer.nextNode(idCorrespondingToPartition4)
      res4 must beSome[Node].which(possibleNodeSet2 must contain(_))
    }

  }
}