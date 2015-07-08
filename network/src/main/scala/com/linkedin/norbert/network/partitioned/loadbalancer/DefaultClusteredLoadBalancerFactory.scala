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

import logging.Logging
import cluster.{Node, InvalidClusterException}
import common.Endpoint
import scala.util.Random

/**
 * Factory class that produces DefaultClusteredLoadBalancers
 */
abstract class DefaultClusteredLoadBalancerFactory[PartitionedId](numPartitions: Int,
                                                                  clusterId: Node => Int,
                                                                  serveRequestsIfPartitionMissing: Boolean = true)
  extends PartitionedLoadBalancerFactory[PartitionedId]
  with Logging {
  /**
   * Returns an instance of <code>PartitionedLoadBalancer</code> after implementing all required API definitions.
   *
   * @param endpoints
   * @return a new <code>PartitionedLoadBalancer</code> instance
   */
  def newLoadBalancer(endpoints: Set[Endpoint]): PartitionedLoadBalancer[PartitionedId] = DefaultClusteredLoadBalancer(endpoints, partitionForId, clusterId, numPartitions, serveRequestsIfPartitionMissing)

  /**
   * Calculates the id of the partition on which the specified <code>Id</code> resides.
   *
   * @param id the <code>Id</code> to map to a partition
   *
   * @return the id of the partition on which the <code>Idrever</code> resides
   */
  def partitionForId(id: PartitionedId): Int = {
    calculateHash(id).abs % numPartitions
  }

  /**
   * Hashes the <code>Id</code> provided. Users must implement this method. The <code>HashFunctions</code>
   * object provides an implementation of the FNV hash which may help in the implementation.
   *
   * @param id the <code>Id</code> to hash
   *
   * @return the hashed value
   */
  protected def calculateHash(id: PartitionedId): Int

  def getNumPartitions(endpoints: Set[Endpoint]): Int

}

object DefaultClusteredLoadBalancer {

  def apply[PartitionedId](endpoints: Set[Endpoint], partitionForId: PartitionedId => Int, clusterId: Node => Int, numPartitions: Int, serveRequestsIfPartitionMissing: Boolean) = {

    //generatePartitionToNodeMap requires the Logging constructor to be called, and so must be evaluated lazily.
    new DefaultClusteredLoadBalancer[PartitionedId] (endpoints, partitionForId, clusterId, numPartitions, serveRequestsIfPartitionMissing) with DefaultLoadBalancerHelper {
      val partitionToNodeMap = generatePartitionToNodeMap(endpoints, numPartitions, serveRequestsIfPartitionMissing)
      val clusterToNodeMap = generateClusterToNodeMap(endpoints)
    }
  }
}

/** This class extends the DefaultPartitionedLoadBalancer to support cluster-awareness, including methods to broadcast to particular clusters,
 *  sending messages to nodes serving specific partitions in specific clusters and so on.
 */
abstract class DefaultClusteredLoadBalancer[PartitionedId](endpoints: Set[Endpoint], partitionForId: PartitionedId => Int, clusterId: Node => Int, numPartitions: Int, serveRequestsIfPartitionMissing: Boolean)
        extends DefaultPartitionedLoadBalancer[PartitionedId](endpoints, partitionForId, numPartitions, serveRequestsIfPartitionMissing) {

  this: PartitionedLoadBalancerHelpers with Logging =>

  val clusterToNodeMap: Map[Int, IndexedSeq[Endpoint]]

  /**
   * Calculates a mapping of nodes to partitions. The nodes should be selected from the given number of replicas.
   * If the numberOfReplicas is zero or grater than maximum number of clusters, this function will choose node from
   * all clusters.
   *
   * @param ids set of partition ids.
   * @param numberOfReplicas number of replica
   * @param capability
   * @param persistentCapability
   * @return a map from node to partition
   */
  override def nodesForPartitionedIdsInNReplicas(ids: Set[PartitionedId], numberOfReplicas: Int,
                                                 capability: Option[Long] = None, persistentCapability: Option[Long] = None): Map[Node, Set[PartitionedId]] =
  {
    // Randomly sorted cluster set.
    val clusterSet = Random.shuffle(clusterToNodeMap.keySet.toList)
    val numReplicas = if (numberOfReplicas > clusterSet.size || numberOfReplicas == 0) clusterSet.size
    else
      numberOfReplicas

    // Pick up the clusters from the randomly sorted set.
    val clusters = clusterSet.slice(0, numReplicas)

    ids.foldLeft(Map[Node, Set[PartitionedId]]().withDefaultValue(Set())) {
      (map, id) =>
        val node = nodeForPartitionInCluster(partitionForId(id), clusters.toSet, capability, persistentCapability)
        map.updated(node, map(node) + id)
    }
  }

  /**
   * Calculates a mapping of nodes in a given cluster to partitions. The nodes should be selected from the given
   * cluster.
   * If clusterId doesn't exist, it will throw InvalidClusterException.
   *
   * @param ids set of partition ids.
   * @param clusterId cluster id
   * @param capability
   * @param persistentCapability
   * @return a map from node to partition
   */
  override def nodesForPartitionedIdsInOneCluster(ids: Set[PartitionedId], clusterId: Int,
                                                  capability: Option[Long] = None, persistentCapability: Option[Long] = None): Map[Node, Set[PartitionedId]] = {

    // Check whether cluster map contains the given cluster id.
    clusterToNodeMap.get(clusterId) match {
      case Some(nodes) => {
        ids.foldLeft(Map[Node, Set[PartitionedId]]().withDefaultValue(Set())) {
          (map, id) =>
            val node = nodeForPartitionInCluster(partitionForId(id), Set(clusterId), true, capability,
              persistentCapability)
            map.updated(node, map(node) + id)
        }
      }
      case None => throw new InvalidClusterException("Unable to satisfy single cluster request, no cluster for id %s"
              .format(clusterId))
    }
  }

  /**
   * Generates the cluster to node map. This methods iterate over the node to generate the cluster unique integer
   * id. It calls the clusterId function which is provided during the instantiation. The clusterId function should
   * generate unique integer id for the cluster containing the node.
   *
   * @param nodes a set of endpoints
   * @return the cluster to node id
   */
  protected def generateClusterToNodeMap(nodes: Set[Endpoint]): Map[Int, IndexedSeq[Endpoint]] = {
    val clusterToNodeMap = (for (n <- nodes) yield (n, clusterId(n.node)))
            .foldLeft(Map.empty[Int, IndexedSeq[Endpoint]]) {
      case (map, (node, key)) => map + (key -> (node +: map.get(key).getOrElse(Vector.empty[Endpoint])))
    }

    clusterToNodeMap
  }

  private def nodeForPartitionInCluster(partitionId: Int, cluster: Set[Int], capability: Option[Long] = None,
                                        persistentCapability: Option[Long] = None): Node =
    nodeForPartitionInCluster(partitionId, cluster, false, capability, persistentCapability)

  private def nodeForPartitionInCluster(partitionId: Int, cluster: Set[Int], isOneCluster: Boolean,
                                        capability: Option[Long], persistentCapability: Option[Long]): Node = {
    partitionToNodeMap.get(partitionId) match {
      case None =>
        throw new NoNodesAvailableException("Unable to satisfy request, no node available for id %s"
                .format(partitionId))
      case Some((endpoints, counter, states)) =>
        val es = endpoints.size
        counter.compareAndSet(java.lang.Integer.MAX_VALUE, 0)
        val idx = counter.getAndIncrement
        var i = idx
        var loopCount = 0
        do {
          val endpoint = endpoints(i % es)
          // Filter the node with the given cluster id. Then, check whether the isOneCluster flag. If this call is for
          // only one cluster, we should not check the node status since it can cause selecting nodes from other
          // clusters.
          if(cluster.contains(clusterId(endpoint.node)) && (isOneCluster || isEndpointViable(capability,persistentCapability,endpoint))) {
            compensateCounter(idx, loopCount, counter)
            return endpoint.node
          }

          i = i + 1
          if (i < 0) i = 0
          loopCount = loopCount + 1
        } while (loopCount <= es)
        compensateCounter(idx, loopCount, counter)
        if (isOneCluster)
          throw new NoNodesAvailableException("Unable to satisfy single cluster request, no node available for id %s"
                  .format(partitionId))
        return endpoints(idx % es).node
    }
  }
}
