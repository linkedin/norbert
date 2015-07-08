package com.linkedin.norbert
package network
package partitioned
package loadbalancer

import org.specs.SpecificationWithJUnit
import cluster.{InvalidClusterException, Node}
import common.Endpoint

import com.linkedin.norbert.cluster.Node
import com.linkedin.norbert.network.common.Endpoint


/**
 * @author: rwang
 * @date: 10/11/12
 * @version: $Revision$
 */

class TestSetCoverPartitionedLoadBalancer extends SpecificationWithJUnit {
  class TestLBF(numPartitions: Int, csr: Boolean = true)
        extends DefaultPartitionedLoadBalancerFactory[Int](numPartitions,csr)
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

  val nodes = Set(Node(1, "node 1", true, Set(0,1,2)),
                  Node(2, "node 2", true, Set(3,4,0)),
                  Node(3, "node 3", true, Set(1,2,3)),
                  Node(4, "node 4", true, Set(4,5,0)),
                  Node(5, "node 5", true, Set(1,2,3)),
                  Node(6, "node 6", true, Set(4,5,0)))


  "Set cover load balancer" should {
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

  }
}