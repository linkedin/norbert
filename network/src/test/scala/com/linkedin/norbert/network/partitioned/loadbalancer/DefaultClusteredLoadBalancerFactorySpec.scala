/*
 * Copyright 2009-2014 LinkedIn, Inc
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

import org.specs.SpecificationWithJUnit
import cluster.{Node, InvalidClusterException}
import common.Endpoint

/**
 * Test cases for <code>DefaultClusteredLoadBalancer</code>. Since the class is implemented based on the
 * <code>DefaultPartitionedLoadBalancer</code>, this test case just checks the additional functionality.
 */
class DefaultClusteredLoadBalancerFactorySpec extends SpecificationWithJUnit {

  case class EId(id: Int)

  implicit def eId2ByteArray(eId: EId): Array[Byte] = BigInt(eId.id).toByteArray

  class EIdDefaultLoadBalancerFactory(numPartitions: Int,
                                      clusterId: Node => Int,
                                      serveRequestsIfPartitionMissing: Boolean)
    extends DefaultClusteredLoadBalancerFactory[EId](numPartitions,
      clusterId,
      serveRequestsIfPartitionMissing) {

    protected def calculateHash(id: EId) = HashFunctions.fnv(id)

    def getNumPartitions(endpoints: Set[Endpoint]) = numPartitions
  }

  class TestEndpoint(val node: Node, var csr: Boolean) extends Endpoint {
    def canServeRequests = csr

    def setCsr(ncsr: Boolean) {
      csr = ncsr
    }
  }

  def toEndpoints(nodes: Set[Node]): Set[Endpoint] = nodes.map(n => new TestEndpoint(n, true))

  def markUnavailable(endpoints: Set[Endpoint], id: Int) {
    endpoints.foreach {
      endpoint =>
        if (endpoint.node.id == id) {
          endpoint.asInstanceOf[TestEndpoint].setCsr(false)
        }
    }
  }

  /**
   * Sample clusterId implementation. This test case assumes that the node id has replica information. The most
   * digits before least three digits are cluster id. Thus, it computes cluster id from node id by dividing 1000.
   * @param node a node.
   * @return cluster id.
   */
  def clusterId(node: Node): Int = node.id / 1000


  val loadBalancerFactory = new EIdDefaultLoadBalancerFactory(20, clusterId, false)

  /**
   * Multi-replica example. The least three digits is used for node id in each replica. The most digits before least
   * three digits for cluster id. For example, 11010 means 10 node in cluster 11. This example closely related with the
   * clusterId method that extract cluster number from node information.
   */
  val nodes = Set(
    Node(1001, "localhost:31313", true, Set(0, 1, 2, 3)),
    Node(1002, "localhost:31313", true, Set(4, 5, 6, 7)),
    Node(1003, "localhost:31313", true, Set(8, 9, 10, 11)),
    Node(1004, "localhost:31313", true, Set(12, 13, 14, 15)),
    Node(1005, "localhost:31313", true, Set(16, 17, 18, 19)),

    Node(2001, "localhost:31313", true, Set(0, 2, 4, 6)),
    Node(2002, "localhost:31313", true, Set(8, 10, 12, 14)),
    Node(2003, "localhost:31313", true, Set(16, 18, 1, 3)),
    Node(2004, "localhost:31313", true, Set(5, 7, 9, 11)),
    Node(2005, "localhost:31313", true, Set(13, 15, 17, 19)),

    Node(3001, "localhost:31313", true, Set(0, 3, 6, 9)),
    Node(3002, "localhost:31313", true, Set(12, 15, 18, 1)),
    Node(3003, "localhost:31313", true, Set(4, 7, 10, 13)),
    Node(3004, "localhost:31313", true, Set(16, 19, 2, 5)),
    Node(3005, "localhost:31313", true, Set(8, 11, 14, 17)),

    Node(4001, "localhost:31313", true, Set(0, 4, 8, 12)),
    Node(4002, "localhost:31313", true, Set(16, 1, 5, 9)),
    Node(4003, "localhost:31313", true, Set(13, 17, 2, 6)),
    Node(4004, "localhost:31313", true, Set(10, 14, 18, 3)),
    Node(4005, "localhost:31313", true, Set(7, 11, 15, 19)),

    Node(5001, "localhost:31313", true, Set(0, 5, 10, 15)),
    Node(5002, "localhost:31313", true, Set(1, 6, 11, 16)),
    Node(5003, "localhost:31313", true, Set(2, 7, 12, 17)),
    Node(5004, "localhost:31313", true, Set(3, 8, 13, 18)),
    Node(5005, "localhost:31313", true, Set(4, 9, 14, 19)),

    Node(10001, "localhost:31313", true, Set(0, 6, 12, 18)),
    Node(10002, "localhost:31313", true, Set(5, 11, 17, 4)),
    Node(10003, "localhost:31313", true, Set(10, 16, 3, 9)),
    Node(10004, "localhost:31313", true, Set(15, 2, 8, 14)),
    Node(10005, "localhost:31313", true, Set(1, 7, 13, 19))
  )

  /**
   * Test case for plain fan-out controls. This test case checks whether the number of clusters is same as the limits
   * of clusters.
   */
  "DefaultClusteredLoadBalancer" should {
    " returns nodes within only N clusters from nodesForPartitionsIdsInNReplicas" in {

      // Prepares load balancer.
      val lb = loadBalancerFactory.newLoadBalancer(toEndpoints(nodes))

      // Prepares id sets.
      val set = (for (id <- 1001 to 2000) yield (id)).foldLeft(Set.empty[EId]) {
        (set, id) => set.+(EId(id))
      }

      // Checks whether return node lists are properly bounded in given number of clusters.
      for (n <- 0 to 6) {
        // Selecting nodes from N clusters.
        val nodesInClusters = lb.nodesForPartitionsIdsInNReplicas(set, n, None, None)

        // Creating a mapping cluster to nodes.
        val selectedClusterSet = nodesInClusters.keySet.foldLeft(Set.empty[Int]) {
          (set, node) => set.+(clusterId(node))
        }

        // Checks whether we have total n different clusters.
        (selectedClusterSet.size) must be_==(if (n == 0) (6) else (n))
      }
    }

    /**
     * Test case for all partition is missing. The implementation should pick up a node even though all nodes are not
     * available. Under multi-replica with rerouting strategy. It is better to return the node rather than throw
     * exception. This test case calls nodesForPartitionsIdsInNReplicas under the condition that all partition zero is
     * missing. However, it should return the set of nodes.
     */
    "nodesForPartitionsIdsInNReplicas not throw NoNodeAvailable if partition is missing." in {
      // TestNode set wraps Node.
      val endpoints = toEndpoints(nodes)

      // Mark all node unavailable for partition zero.
      markUnavailable(endpoints, 1001)
      markUnavailable(endpoints, 2001)
      markUnavailable(endpoints, 3001)
      markUnavailable(endpoints, 4001)
      markUnavailable(endpoints, 5001)
      markUnavailable(endpoints, 10001)

      // Prepares load balancer.
      val lb = loadBalancerFactory.newLoadBalancer(endpoints)

      // Prepares id sets.
      val set = (for (id <- 2001 to 2100) yield (id)).foldLeft(Set.empty[EId]) {
        (set, id) => set.+(EId(id))
      }


      for (n <- 0 to 6) {
        // Returns nodes even though some partition is not available.
        lb.nodesForPartitionsIdsInNReplicas(set, n, None, None) mustNot throwA[NoNodesAvailableException]
      }
    }

    /**
     * Test case for sending one cluster by specifying the cluster id.
     */
    " returns nodes within only one clusters from nodesForPartitionsIdsInNReplicas" in {

      // Prepares load balancer.
      val lb = loadBalancerFactory.newLoadBalancer(toEndpoints(nodes))

      // Prepares id sets.
      val set = (for (id <- 2001 to 3000) yield (id)).foldLeft(Set.empty[EId]) {
        (set, id) => set.+(EId(id))
      }

      // Prepares all cluster id set
      val clusterSet:Set[Int] = {
        (for (node <- nodes) yield clusterId(node)).foldLeft(Set[Int]()) {
          case (set, key) => set + key
        }
      }

      // Checks whether we return nodes from specified cluster.
      for (id <- clusterSet) {
        // Selecting nodes from the cluster specified as id.
        val nodesInClusters = lb.nodesForPartitionsIdsInOneCluster(set, id, None, None)

        // Creating a mapping cluster to nodes.
        val selectedClusterSet = nodesInClusters.keySet.foldLeft(Set.empty[Int]) {
          (set, node) => set.+(clusterId(node))
        }

        // Checks whether we have total one cluster
        (selectedClusterSet.size) must be_==(1)

        // Checks whether the cluster id is same as given id
        (selectedClusterSet) must be_==(Set(id))
      }
    }

    /**
     * Test case for sending one cluster with invalid cluster id.
     */
    "nodesForPartitionsIdsInOneCluster should throw NoClusterException if there is no matching cluster id" in {

      // Prepares load balancer.
      val lb = loadBalancerFactory.newLoadBalancer(toEndpoints(nodes))

      // Prepares id sets.
      val set = (for (id <- 2001 to 3000) yield (id)).foldLeft(Set.empty[EId]) {
        (set, id) => set.+(EId(id))
      }

      val invalidClusterId = -1

     lb.nodesForPartitionsIdsInOneCluster(set, invalidClusterId, None, None) must throwA[InvalidClusterException]


    }
  }
}
