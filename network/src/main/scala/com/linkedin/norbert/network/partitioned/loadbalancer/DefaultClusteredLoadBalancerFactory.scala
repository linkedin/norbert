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
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import scala.util.Random
import scala.util.control.Breaks._

/**
 * This class is intended for applications where there is a mapping from partitions -> servers able to respond to those requests. Requests are round-robined
 * between the partitions
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
  def newLoadBalancer(endpoints: Set[Endpoint]): PartitionedLoadBalancer[PartitionedId] = new PartitionedLoadBalancer[PartitionedId] with DefaultLoadBalancerHelper {
    val partitionToNodeMap = generatePartitionToNodeMap(endpoints, numPartitions, serveRequestsIfPartitionMissing)
    val clusterToNodeMap = generateClusterToNodeMap(endpoints)

    val setCoverCounter = new AtomicInteger(0)

    def nextNode(id: PartitionedId, capability: Option[Long] = None, persistentCapability: Option[Long] = None) = nodeForPartition(partitionForId(id), capability, persistentCapability)


    def nodesForPartitionedId(id: PartitionedId, capability: Option[Long] = None, persistentCapability: Option[Long] = None) = {
      partitionToNodeMap.getOrElse(partitionForId(id), (Vector.empty[Endpoint], new AtomicInteger(0), new Array[AtomicBoolean](0)))._1.filter(_.node.isCapableOf(capability, persistentCapability)).toSet.map {
        (endpoint: Endpoint) => endpoint.node
      }
    }

    def nodesForOneReplica(id: PartitionedId, capability: Option[Long] = None, persistentCapability: Option[Long] = None) = {
      nodesForPartitions(id, partitionToNodeMap, capability, persistentCapability)
    }

    def nodesForPartitions(id: PartitionedId, partitions: Set[Int], capability: Option[Long] = None, persistentCapability: Option[Long] = None) = {
      nodesForPartitions(id, partitionToNodeMap.filterKeys(partitions contains _), capability, persistentCapability)
    }

    def nodesForPartitions(id: PartitionedId, partitionToNodeMap: Map[Int, (IndexedSeq[Endpoint], AtomicInteger, Array[AtomicBoolean])], capability: Option[Long], persistentCapability: Option[Long]) = {
      partitionToNodeMap.keys.foldLeft(Map.empty[Node, Set[Int]]) {
        (map, partition) =>
          val nodeOption = nodeForPartition(partition, capability, persistentCapability)
          if (nodeOption.isDefined) {
            val n = nodeOption.get
            map + (n -> (map.getOrElse(n, Set.empty[Int]) + partition))
          } else if (serveRequestsIfPartitionMissing) {
            log.warn("Partition %s is unavailable, attempting to continue serving requests to other partitions.".format(partition))
            map
          } else
            throw new InvalidClusterException("Partition %s is unavailable, cannot serve requests.".format(partition))
      }
    }

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

      // Pick up the clusters from the randomly sorted set.
      clusterToNodeMap.get(clusterId) match {
        case Some(nodes) => {
          ids.foldLeft(Map[Node, Set[PartitionedId]]().withDefaultValue(Set())) {
            (map, id) =>
              val node = nodeForPartitionInCluster(partitionForId(id), Set(clusterId), true, capability,
                  persistentCapability)
              map.updated(node, map(node) + id)
          }
        }
        case None => throw new InvalidClusterException("Unable to satisfy request, no cluster for id %s"
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
            if(cluster.contains(clusterId(endpoint.node)) && (isOneCluster || (endpoint.canServeRequests
                && endpoint.node.isCapableOf(capability, persistentCapability)))) {
              compensateCounter(idx, loopCount, counter);
              return endpoint.node
            }

            i = i + 1
            if (i < 0) i = 0
            loopCount = loopCount + 1
          } while (loopCount <= es)
          compensateCounter(idx, loopCount, counter);
          if (isOneCluster)
            throw new NoNodesAvailableException("Unable to satisfy request, no node available for id %s"
                .format(partitionId))
          return endpoints(idx % es).node
      }
    }

    /**
     * Use greedy set cover to minimize the nodes that serve the requested partitioned Ids
     *
     * @param partitionedIds
     * @param capability
     * @param persistentCapability
     * @return
     */
    override def nodesForPartitionedIds(partitionedIds: Set[PartitionedId], capability: Option[Long], persistentCapability: Option[Long] = None) = {

      // calculates partition Ids from the set of partitioned Ids
      val partitionsMap = partitionedIds.foldLeft(Map.empty[Int, Set[PartitionedId]]) {
        case (map, id) =>
          val partition = partitionForId(id)
          map + (partition -> (map.getOrElse(partition, Set.empty[PartitionedId]) + id))
      }

      // set to be covered
      var partitionIds = partitionsMap.keys.toSet[Int]
      val res = collection.mutable.Map.empty[Node, collection.Set[Int]]

      breakable {
        while (!partitionIds.isEmpty) {
          var intersect = Set.empty[Int]
          var endpoint: Endpoint = null

          // take one element in the set, locate only nodes that serving this partition
          partitionToNodeMap.get(partitionIds.head) match {
            case None =>
              break
            case Some((endpoints, counter, states)) =>
              val es = endpoints.size
              counter.compareAndSet(java.lang.Integer.MAX_VALUE, 0)
              val idx = counter.getAndIncrement % es
              var i = idx

              // This is a modified version of greedy set cover algorithm, instead of finding the node that covers most of
              // the partitionIds set, we only check it across nodes that serving the selected partition. This guarantees
              // we will pick a node at least cover 1 more partition, but also in case of multiple replicas of partitions,
              // this helps to locate nodes long to the same replica.
              breakable {
                do {
                  val ep = endpoints(i)

                  // perform intersection between the set to be covered and the set the node is covering
                  val s = ep.node.partitionIds intersect partitionIds

                  // record the largest intersect
                  if (s.size > intersect.size && ep.canServeRequests && ep.node.isCapableOf(capability, persistentCapability)) {
                    intersect = s
                    endpoint = ep
                    if (partitionIds.size == s.size)
                      break
                  }
                  i = (i + 1) % es
                } while (i != idx)
              }
          }

          if (endpoint == null) {
            if (serveRequestsIfPartitionMissing) {
              intersect = intersect + partitionIds.head
            }
            else
              throw new NoNodesAvailableException("Unable to satisfy request, no node available for partition Id %s".format(partitionIds.head))
          } else
            res += (endpoint.node -> intersect)

          // remove covered set; remove the node providing that coverage
          partitionIds = (partitionIds -- intersect)
        }
      }

      res.foldLeft(Map.empty[Node, Set[PartitionedId]]) {
        case (map, (n, pIds)) => {
          map + (n -> pIds.foldLeft(Set.empty[PartitionedId]) {
            case (s, pId) =>
              s ++ partitionsMap.getOrElse(pId, Set.empty[PartitionedId])
          })
        }
      }
    }
  }

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
