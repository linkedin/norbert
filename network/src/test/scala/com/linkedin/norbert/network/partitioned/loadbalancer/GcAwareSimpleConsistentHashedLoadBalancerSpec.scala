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

import com.linkedin.norbert.cluster.Node
import com.linkedin.norbert.network.common.Endpoint
import com.linkedin.norbert.network.partitioned.loadbalancer.gcaware.GcAwareSimpleConsistentHashedLoadBalancerFactory
import com.linkedin.norbert.util.WaitFor
import org.specs2.mutable.SpecificationWithJUnit


/**
  * @author: sishah
  * @date: 06/18/15
  * @version: 1.0
  *
  * TODO sishah: Flesh this out
  */
class GcAwareSimpleConsistentHashedLoadBalancerSpec extends SpecificationWithJUnit with WaitFor {

  val cycleTime = 6000
  val slotTime = 2000

  class TestLBF extends GcAwareSimpleConsistentHashedLoadBalancerFactory[Int](10,
    (id: Int) => HashFunctions.fnv(BigInt(id).toByteArray),
    (str: String) => str.hashCode(), cycleTime, slotTime)

  val loadBalancerFactory = new TestLBF()

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

  val node1 = new Node(1, "node 1", true, Set(0, 1, 2), None, None, Some(0))
  val node2 = new Node(2, "node 2", true, Set(3, 4, 5), None, None, Some(0))
  val node3 = new Node(3, "node 3", true, Set(1, 2, 3), None, None, Some(1))
  val node4 = new Node(4, "node 4", true, Set(4, 5, 0), None, None, Some(1))
  val node5 = new Node(5, "node 5", true, Set(1, 2, 3), None, None, Some(2))
  val node6 = new Node(6, "node 6", true, Set(4, 5, 0), None, None, Some(2))

  val nodes = Set(node1, node2, node3, node4, node5, node6)

  "GC-aware SimpleConsistentHashed load balancer" should {

    "be sticky WHILE not returning a Node which is currently GCing" in {
      val loadbalancer = loadBalancerFactory.newLoadBalancer(toEndpoints(nodes))
      while (System.currentTimeMillis() % cycleTime != 0) {}

      val idCorrespondingToPartition1 = 1210
      val idCorrespondingToPartition4 = 1318

      val possibleNodeSet = scala.collection.mutable.Set(node3, node5)
      val res = loadbalancer.nextNode(idCorrespondingToPartition1)
      res must beSome[Node].which(possibleNodeSet must contain(_))
      possibleNodeSet remove res.get
      val res2 = loadbalancer.nextNode(idCorrespondingToPartition1)
      res2 must be_==(res)

      while (System.currentTimeMillis() % slotTime != 0) {}
      val possibleNodeSet2 = scala.collection.mutable.Set(node2, node6)
      val res3 = loadbalancer.nextNode(idCorrespondingToPartition4)
      res3 must beSome[Node].which(possibleNodeSet2 must contain(_))
      possibleNodeSet2 remove res3.get
      val res4 = loadbalancer.nextNode(idCorrespondingToPartition4)
      res4 must be_==(res3)
    }
  }
}
