package com.linkedin.norbert
package network
package partitioned
package loadbalancer

import com.linkedin.norbert.network.partitioned.loadbalancer.gcaware.GcAwareClusteredLoadBalancerFactory
import org.specs2.mutable.SpecificationWithJUnit
import com.linkedin.norbert.cluster.{InvalidClusterException, Node}
import com.linkedin.norbert.util.WaitFor
import common.Endpoint
import org.specs2.specification.Scope

/**
  * Created by sishah on 7/20/15.
  */
class GcAwareClusteredLoadBalancerSpec extends SpecificationWithJUnit with WaitFor {

  trait GcAwareClusteredLoadBalancerSetup extends Scope {
    val cycleTime = 26400
    val slotTime = 2400

    // Buffer time, in milliseconds, to prevent test cases failing at slot transition boundaries.
    // i.e. we wait this amount of time into a particular slot before testing
    val slackTime = 10

    case class EId(id: Int)

    implicit def eId2ByteArray(eId: EId): Array[Byte] = BigInt(eId.id).toByteArray

    class EIdDefaultLoadBalancerFactory(numPartitions: Int,
      clusterId: Node => Int,
      serveRequestsIfPartitionMissing: Boolean)
      extends GcAwareClusteredLoadBalancerFactory[EId](numPartitions,
        cycleTime,
        slotTime,
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
      *
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
      Node(1001, "localhost:31313", true, Set(0, 1, 2, 3), None, None, Some(0)),
      Node(1002, "localhost:31313", true, Set(4, 5, 6, 7), None, None, Some(0)),
      Node(1003, "localhost:31313", true, Set(8, 9, 10, 11), None, None, Some(0)),
      Node(1004, "localhost:31313", true, Set(12, 13, 14, 15), None, None, Some(0)),
      Node(1005, "localhost:31313", true, Set(16, 17, 18, 19), None, None, Some(0)),

      Node(2001, "localhost:31313", true, Set(0, 2, 4, 6), None, None, Some(1)),
      Node(2002, "localhost:31313", true, Set(8, 10, 12, 14), None, None, Some(1)),
      Node(2003, "localhost:31313", true, Set(16, 18, 1, 3), None, None, Some(1)),
      Node(2004, "localhost:31313", true, Set(5, 7, 9, 11), None, None, Some(1)),
      Node(2005, "localhost:31313", true, Set(13, 15, 17, 19), None, None, Some(1)),

      Node(3001, "localhost:31313", true, Set(0, 3, 6, 9), None, None, Some(2)),
      Node(3002, "localhost:31313", true, Set(12, 15, 18, 1), None, None, Some(2)),
      Node(3003, "localhost:31313", true, Set(4, 7, 10, 13), None, None, Some(2)),
      Node(3004, "localhost:31313", true, Set(16, 19, 2, 5), None, None, Some(2)),
      Node(3005, "localhost:31313", true, Set(8, 11, 14, 17), None, None, Some(2)),

      Node(4001, "localhost:31313", true, Set(0, 4, 8, 12), None, None, Some(3)),
      Node(4002, "localhost:31313", true, Set(16, 1, 5, 9), None, None, Some(3)),
      Node(4003, "localhost:31313", true, Set(13, 17, 2, 6), None, None, Some(3)),
      Node(4004, "localhost:31313", true, Set(10, 14, 18, 3), None, None, Some(3)),
      Node(4005, "localhost:31313", true, Set(7, 11, 15, 19), None, None, Some(3)),

      Node(5001, "localhost:31313", true, Set(0, 5, 10, 15), None, None, Some(4)),
      Node(5002, "localhost:31313", true, Set(1, 6, 11, 16), None, None, Some(4)),
      Node(5003, "localhost:31313", true, Set(2, 7, 12, 17), None, None, Some(4)),
      Node(5004, "localhost:31313", true, Set(3, 8, 13, 18), None, None, Some(4)),
      Node(5005, "localhost:31313", true, Set(4, 9, 14, 19), None, None, Some(4)),

      Node(10001, "localhost:31313", true, Set(0, 6, 12, 18), None, None, Some(9)),
      Node(10002, "localhost:31313", true, Set(5, 11, 17, 4), None, None, Some(9)),
      Node(10003, "localhost:31313", true, Set(10, 16, 3, 9), None, None, Some(9)),
      Node(10004, "localhost:31313", true, Set(15, 2, 8, 14), None, None, Some(9)),
      Node(10005, "localhost:31313", true, Set(1, 7, 13, 19), None, None, Some(9)),

      Node(11001, "localhost:31313", true, Set(0, 7, 14, 13), None, None, Some(10)),
      Node(11002, "localhost:31313", true, Set(1, 8, 15, 19), None, None, Some(10)),
      Node(11003, "localhost:31313", true, Set(2, 9, 16, 3), None, None, Some(10)),
      Node(11004, "localhost:31313", true, Set(4, 10, 17, 5), None, None, Some(10)),
      Node(11005, "localhost:31313", true, Set(6, 11, 18, 12), None, None, Some(10))
    )

    val slotsAssigned = Set(0, 1, 2, 3, 4, 9, 10)


    def getSlotForCluster(cluster: Int) = cluster - 1

    def getClusterForSlot(offset: Int) = offset + 1

    def getTimeForSlotToEnd(nodeOffset: Int): Int = {

      //How far along are we in the current cycle
      val timeOffsetWithinCycle: Long = System.currentTimeMillis() % cycleTime

      //How far along in the cycle should this node GC
      val nextGcTimeOffsetWithinCycle: Long = (nodeOffset + 1) * slotTime

      if (nextGcTimeOffsetWithinCycle > timeOffsetWithinCycle)
        (nextGcTimeOffsetWithinCycle - timeOffsetWithinCycle).asInstanceOf[Int]
      else
        0

    }

    def currentSlot: Int = {
      ((System.currentTimeMillis() % cycleTime) / slotTime).asInstanceOf[Int]
    }
  }

  /**
    * Test case for plain fan-out controls. This test case checks whether the number of clusters is same as the limits
    * of clusters.
    */
  "GcAwareClusteredLoadBalancer" should {
    " returns nodes within only N clusters from nodesForPartitionsIdsInNReplicas" in new GcAwareClusteredLoadBalancerSetup {

      // Prepares load balancer.
      val lb = loadBalancerFactory.newLoadBalancer(toEndpoints(nodes))

      // Prepares id sets.
      val set = (for (id <- 1001 to 2000) yield (id)).foldLeft(Set.empty[EId]) {
        (set, id) => set.+(EId(id))
      }

      // Checks whether return node lists are properly bounded in given number of clusters.
      // Not doing 1 as if the cluster selected is GCing, fanout control will be aborted.
      // Not doing 7 (or 0) as this will never be possible in a GC aware setting - 1 cluster is always GCing
      for (n <- 2 to 6) {
        // Selecting nodes from N clusters.
        val nodesInClusters = lb.nodesForPartitionedIdsInNReplicas(set, n, None, None)

        // Creating a mapping cluster to nodes.
        val selectedClusterSet = nodesInClusters.keySet.foldLeft(Set.empty[Int]) {
          (set, node) => set.+(clusterId(node))
        }

        // Checks whether we have total n different clusters.
        (selectedClusterSet.size) must be_<=(n)
      }
    }

    /**
      * Note: This test will have to change if the cluster based GC approach used now, changes.
      * Maybe this test isn't necessary?
      */
    " ignores the GCing cluster when asked to fan out to <number of clusters> replicas" in new GcAwareClusteredLoadBalancerSetup {

      // Prepares load balancer.
      val lb = loadBalancerFactory.newLoadBalancer(toEndpoints(nodes))

      // Prepares id sets.
      val set = (for (id <- 1001 to 1500) yield (id)).foldLeft(Set.empty[EId]) {
        (set, id) => set.+(EId(id))
      }

      while ((System.currentTimeMillis() - slackTime) % slotTime != 0 || !(slotsAssigned contains currentSlot)) {}

      val nodesInClusters = lb.nodesForPartitionedIdsInNReplicas(set, 7, None, None)

      // Creating a mapping cluster to nodes.
      val selectedClusterSet = nodesInClusters.keySet.foldLeft(Set.empty[Int]) {
        (set, node) => set.+(clusterId(node))
      }

      // Checks whether we have total n different clusters.
      (selectedClusterSet.size) must be_==(6)
    }

    /**
      * Test case for all partition is missing. The implementation should pick up a node even though all nodes are not
      * available. Under multi-replica with rerouting strategy. It is better to return the node rather than throw
      * exception. This test case calls nodesForPartitionsIdsInNReplicas under the condition that all partition zero is
      * missing. However, it should return the set of nodes.
      */
    "nodesForPartitionsIdsInNReplicas not throw NoNodeAvailable if partition is missing." in new GcAwareClusteredLoadBalancerSetup {
      // TestNode set wraps Node.
      val endpoints = toEndpoints(nodes)

      // Mark all node unavailable for partition zero.
      markUnavailable(endpoints, 1001)
      markUnavailable(endpoints, 2001)
      markUnavailable(endpoints, 3001)
      markUnavailable(endpoints, 4001)
      markUnavailable(endpoints, 5001)
      markUnavailable(endpoints, 10001)
      markUnavailable(endpoints, 11001)

      // Prepares load balancer.
      val lb = loadBalancerFactory.newLoadBalancer(endpoints)

      // Prepares id sets.
      val set = (for (id <- 2001 to 2100) yield (id)).foldLeft(Set.empty[EId]) {
        (set, id) => set.+(EId(id))
      }


      for (n <- 1 to 6) {
        // Returns nodes even though some partition is not available.
        lb.nodesForPartitionedIdsInNReplicas(set, n, None, None) must_!= throwA[NoNodesAvailableException]
      }
    }

    /**
      * Test case for sending one cluster by specifying the cluster id, while that cluster IS NOT GCing.
      */
    " returns nodes within only one clusters from nodesForPartitionsIdsInNReplicas" in new GcAwareClusteredLoadBalancerSetup {

      // Prepares load balancer.
      val lb = loadBalancerFactory.newLoadBalancer(toEndpoints(nodes))

      // Prepares id sets.
      val set = (for (id <- 2001 to 3000) yield (id)).foldLeft(Set.empty[EId]) {
        (set, id) => set.+(EId(id))
      }

      // Prepares all cluster id set
      val clusterSet: Set[Int] = {
        (for (node <- nodes) yield clusterId(node)).foldLeft(Set[Int]()) {
          case (set, key) => set + key
        }
      }

      // Checks whether we return nodes from specified cluster.
      for (id <- clusterSet) {

        val currSlot = (System.currentTimeMillis() % cycleTime) / slotTime
        if (currSlot == getSlotForCluster(id)) {
          val timeForSlotToEnd = getTimeForSlotToEnd(currSlot.asInstanceOf[Int])
          if (timeForSlotToEnd != 0)
            waitFor((timeForSlotToEnd.asInstanceOf[Int] + slackTime).ms)
        }

        // Selecting nodes from the cluster specified as id.
        val nodesInClusters = lb.nodesForPartitionedIdsInOneCluster(set, id, None, None)

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
      * Test case for sending one cluster by specifying the cluster id, while that cluster IS GCing.
      */
    " throws a NoNodesAvailableException if asked for nodes within GCing cluster" in new GcAwareClusteredLoadBalancerSetup {

      // Prepares load balancer.
      val lb = loadBalancerFactory.newLoadBalancer(toEndpoints(nodes))

      // Prepares id sets.
      val set = (for (id <- 2001 to 3000) yield id).foldLeft(Set.empty[EId]) {
        (set, id) => set.+(EId(id))
      }

      // Prepares all cluster id set
      val clusterSet: Set[Int] = {
        (for (node <- nodes) yield clusterId(node)).foldLeft(Set[Int]()) {
          case (set, key) => set + key
        }
      }

      while (!clusterSet.contains(getClusterForSlot(((System.currentTimeMillis() % cycleTime) / slotTime).asInstanceOf[Int]))
        || (getTimeForSlotToEnd(((System.currentTimeMillis() % cycleTime) / slotTime).asInstanceOf[Int]) < 1000)) {}

      val clusterToTest = getClusterForSlot(((System.currentTimeMillis() % cycleTime) / slotTime).asInstanceOf[Int])

      // Selecting nodes from the cluster specified as id.
      val nodesInClusters = lb.nodesForPartitionedIdsInOneCluster(set, clusterToTest, None, None) must throwA[NoNodesAvailableException]

    }

    /**
      * Test case for sending one cluster with invalid cluster id.
      */
    "nodesForPartitionsIdsInOneCluster should throw NoClusterException if there is no matching cluster id" in new GcAwareClusteredLoadBalancerSetup {

      // Prepares load balancer.
      val lb = loadBalancerFactory.newLoadBalancer(toEndpoints(nodes))

      // Prepares id sets.
      val set = (for (id <- 2001 to 3000) yield (id)).foldLeft(Set.empty[EId]) {
        (set, id) => set.+(EId(id))
      }

      val invalidClusterId = -1

      lb.nodesForPartitionedIdsInOneCluster(set, invalidClusterId, None, None) must throwA[InvalidClusterException]
    }
  }
}