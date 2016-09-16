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
package com.linkedin.norbert.network.partitioned.loadbalancer.gcaware

import com.linkedin.norbert.cluster.{InvalidClusterException, Node}
import com.linkedin.norbert.logging.Logging
import com.linkedin.norbert.network.NoNodesAvailableException
import com.linkedin.norbert.network.common.Endpoint
import com.linkedin.norbert.network.partitioned.loadbalancer.{DefaultClusteredLoadBalancer, DefaultClusteredLoadBalancerFactory, PartitionedLoadBalancer}
import com.linkedin.norbert.norbertutils.SystemClockComponent

import scala.util.Random

/**
 * Extends DefaultClusteredLoadBalancer to add GC-awareness.
 * Nodes are additionally filtered to exclude those that are currently garbage collecting.
 *
 * @param cycleTime: The time period (in milliseconds) in which each node in the data center undergoes
 *                   garbage collection exactly once
 * @param slotTime: The time (in milliseconds) for the nodes in one cluster to finish pending requests
 *                  (SLA time) + the time to garbage collect (GC time).
 *
 *
 *    Offset 2 ------------------------------->
 *
 *    Offset 0 --->                                                                     <- Slot time ->
 *
 *                <-------------|-------------|-------------|-------------|-------------|------------->
 *
 *                <- Cycle Time ---------------------------------------------------------------------->
 *
 *
 * A node n is currently undergoing garbage collection if:
 *         [ [currentTime % cycleTime] / slotTime ]  == n.offset
 */
abstract class GcAwareClusteredLoadBalancerFactory[PartitionedId](numPartitions: Int, cycleTime: Int, slotTime: Int, clusterId: Node => Int, serveRequestsIfPartitionMissing: Boolean = true)
        extends DefaultClusteredLoadBalancerFactory[PartitionedId](numPartitions, clusterId, serveRequestsIfPartitionMissing) with Logging {

  override def newLoadBalancer(endpoints: Set[Endpoint]): PartitionedLoadBalancer[PartitionedId] =
    GcAwareClusteredLoadBalancer(endpoints, partitionForId, clusterId, cycleTime, slotTime, numPartitions, serveRequestsIfPartitionMissing)

}

object GcAwareClusteredLoadBalancer {

  def apply[PartitionedId](endpoints: Set[Endpoint], partitionForId: PartitionedId => Int, clusterId: Node => Int, cycleTime: Int, slotTime: Int, numPartitions: Int, serveRequestsIfPartitionMissing: Boolean) = {

    //generatePartitionToNodeMap requires the Logging constructor to be called, and so must be evaluated lazily.
    new GcAwareClusteredLoadBalancer[PartitionedId](endpoints, partitionForId, clusterId, cycleTime, slotTime, numPartitions, serveRequestsIfPartitionMissing) {

      validateOffsets(endpoints)

      val partitionToNodeMap = generatePartitionToNodeMap(endpoints, numPartitions, serveRequestsIfPartitionMissing)
      val clusterToNodeMap = generateClusterToNodeMap(endpoints)

      val gcCycleTime = cycleTime
      val gcSlotTime = slotTime
    }
  }
}

abstract class GcAwareClusteredLoadBalancer[PartitionedId](endpoints: Set[Endpoint], partitionForId: PartitionedId => Int, clusterId: Node => Int, cycleTime: Int, slotTime: Int, numPartitions: Int, serveRequestsIfPartitionMissing: Boolean)
        extends DefaultClusteredLoadBalancer[PartitionedId](endpoints, partitionForId, clusterId, numPartitions, serveRequestsIfPartitionMissing) with GcAwarePartitionedLoadBalancerHelper with SystemClockComponent {


  //Not used - leaving in in case need arises in the future to ensure <= N replicas, or throw exception
  def ensureNodesForPartitionedIdsInNReplicas(ids: Set[PartitionedId], numberOfReplicas: Int,
                                                 capability: Option[Long] = None, persistentCapability: Option[Long] = None): Map[Node, Set[PartitionedId]] = {
    // Randomly sorted cluster set.
    val clusterSet = Random.shuffle(clusterToNodeMap.keySet.toList)
    val numReplicas = if (numberOfReplicas > clusterSet.size || numberOfReplicas == 0) clusterSet.size
    else
      numberOfReplicas

    // Pick up the clusters from the randomly sorted set.
    val clusters = clusterSet.slice(0, numReplicas)
    val remainingClusters = clusterSet diff clusters
    var pendingIds = Set[PartitionedId]()

    var clusterToPartitionsServed = Map[Int, Set[PartitionedId]]().withDefaultValue(Set[PartitionedId]())

    val nodesToPartitionedIds: Map[Node, Set[PartitionedId]] =
      ids.foldLeft(Map[Node, Set[PartitionedId]]().withDefaultValue(Set())) {
      (map, id) => {

        try {
          val node = nodeForPartitionInCluster(partitionForId(id), clusters.toSet, capability, persistentCapability)
          map.updated(node, map(node) + id)
          val clusterForNode = clusterId(node)
          clusterToPartitionsServed = clusterToPartitionsServed.updated(clusterForNode, clusterToPartitionsServed(clusterForNode) + id)
          map
        }
        catch {
          case e: NoNodesAvailableException =>
            pendingIds += id
            map
        }
      }

    }

    if (pendingIds.isEmpty) {
      return nodesToPartitionedIds
    }

    // Try once to find a replacement cluster
    // Tries to swap out a currently selected cluster with one of the remaining ones that can satisfy both that cluster-
    // to-be-removed's partitionedIds + the pending ones, for which we couldn't locate a cluster at all.
    // To review: This takes O([fanout-number-of-clusters] * [remaining-number]), and may be too expensive
    //            On the other hand, this will never probably be hit, because in production we are almost guaranteed
    //            to find a sub-section of the fanout-clusters that can service the request, even with GC awareness.
    //            On the other hand, in EI2, this will quickly fix a bad cluster selection, wherein a GCing cluster was
    //            randomly selected.
    //            Alternatives include picking a reasonable subsection (maybe just 1) of the remaining clusters to
    //            check against, or just losing this bit of code and defaulting to cluster unaware.

    if (remainingClusters.nonEmpty) {
      clusterToPartitionsServed foreach {

        case (clusterToRemove, partitionList) =>

          remainingClusters foreach {

            candidateCluster =>

              try {
                val nodes = nodesForPartitionedIdsInOneCluster(partitionList ++ pendingIds, candidateCluster, capability, persistentCapability)

                return nodesToPartitionedIds -- (clusterToNodeMap(clusterToRemove) map ((ep: Endpoint) => ep.node)) ++ nodes
              }
              catch {
                case e:InvalidClusterException =>
              }
          }
      }
    }

    throw new NoNodesAvailableException("Couldn't generate a result within " + numberOfReplicas + " clusters")

  }

  // This overrides the logic used by the defaultClusteredLoadbalancer to ensure only those nodes that are not GCing are
  // chosen. If a 'nodesInOneCluster' request comes here, and the cluster in question is GCing, this will fail,
  // ensuring that that cluster doesn't receive a request
  override def checkIfOneClusterOrEndpointViable(isOneCluster: Boolean, capability: Option[Long], persistentCapability: Option[Long], endpoint: Endpoint): Boolean = {
    (isOneCluster || isEndpointViable(capability, persistentCapability, endpoint)) && (endpoint.node.offset.isEmpty || !isCurrentlyDownToGC(endpoint.node.offset.get))
  }

}