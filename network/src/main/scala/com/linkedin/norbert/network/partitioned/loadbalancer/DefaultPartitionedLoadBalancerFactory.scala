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
package com.linkedin.norbert
package network
package partitioned
package loadbalancer

import java.util
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import com.linkedin.norbert.cluster.{InvalidClusterException, Node}
import com.linkedin.norbert.logging.Logging
import com.linkedin.norbert.network.common.Endpoint

import scala.util.control.Breaks._

/**
 * This class is intended for applications where there is a mapping from partitions -> servers able to respond to those requests. Requests are round-robined
 * between the partitions
 */
abstract class DefaultPartitionedLoadBalancerFactory[PartitionedId](numPartitions: Int, serveRequestsIfPartitionMissing: Boolean = true) extends PartitionedLoadBalancerFactory[PartitionedId] with Logging {
  def newLoadBalancer(endpoints: Set[Endpoint]): PartitionedLoadBalancer[PartitionedId] = new PartitionedLoadBalancer[PartitionedId] with DefaultLoadBalancerHelper {
    val partitionToNodeMap = generatePartitionToNodeMap(endpoints, numPartitions, serveRequestsIfPartitionMissing)

    val setCoverCounter = new AtomicInteger(0)

    def nextNode(id: PartitionedId, capability: Option[Long] = None, persistentCapability: Option[Long] = None) = nodeForPartition(partitionForId(id), capability, persistentCapability)


    def nodesForPartitionedId(id: PartitionedId, capability: Option[Long] = None, persistentCapability: Option[Long] = None) = {
      partitionToNodeMap.getOrElse(partitionForId(id), (Vector.empty[Endpoint], new AtomicInteger(0), new Array[AtomicBoolean](0)))._1.filter(_.node.isCapableOf(capability, persistentCapability)).toSet.map
      { (endpoint: Endpoint) => endpoint.node }
    }

    def nodesForOneReplica(id: PartitionedId, capability: Option[Long] = None, persistentCapability: Option[Long] = None) = {
      nodesForPartitions(id, partitionToNodeMap, capability, persistentCapability)
    }

    def nodesForPartitions(id: PartitionedId, partitions: Set[Int], capability: Option[Long] = None, persistentCapability: Option[Long] = None) = {
      nodesForPartitions(id, partitionToNodeMap.filterKeys(partitions contains _), capability, persistentCapability)
    }

    def nodesForPartitions(id: PartitionedId, partitionToNodeMap: Map[Int, (IndexedSeq[Endpoint], AtomicInteger, Array[AtomicBoolean])], capability: Option[Long], persistentCapability: Option[Long]) = {
      partitionToNodeMap.keys.foldLeft(Map.empty[Node, Set[Int]]) { (map, partition) =>
        val nodeOption = nodeForPartition(partition, capability, persistentCapability)
        if(nodeOption.isDefined) {
          val n = nodeOption.iterator.next()
          map + (n -> (map.getOrElse(n, Set.empty[Int]) + partition))
        } else if(serveRequestsIfPartitionMissing) {
          log.warn("Partition %s is unavailable, attempting to continue serving requests to other partitions.".format(partition))
          map
        } else
          throw new InvalidClusterException("Partition %s is unavailable, cannot serve requests.".format(partition))
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
      val partitionsMap = partitionedIds.foldLeft(Map.empty[Int, Set[PartitionedId]])
      { case (map, id) =>
        val partition = partitionForId(id)
        map + (partition -> (map.getOrElse(partition, Set.empty[PartitionedId]) + id))
      }

      // set to be covered
      var partitionIds = partitionsMap.keys.toSet[Int]
      val res = collection.mutable.Map.empty[Node, collection.Set[Int]]

      breakable {
        while (!partitionIds.isEmpty) {
          var intersect = Set.empty[Int]
          var endpoint : Endpoint =  null

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
                  if (s.size > intersect.size && ep.canServeRequests && ep.node.isCapableOf(capability, persistentCapability))
                  {
                    intersect = s
                    endpoint = ep
                    if (partitionIds.size == s.size)
                      break
                  }
                  i = (i+1 ) % es
                } while(i != idx)
              }
          }

          if (endpoint == null)
          {
            if (serveRequestsIfPartitionMissing)
            {
              intersect = intersect + partitionIds.head
            }
            else
              throw new NoNodesAvailableException("Unable to satisfy request, no node available for partition Id %s".format(partitionIds.head))
          } else
            res += (endpoint.node -> intersect)

          // remove covered set; remove the node providing that coverage
          partitionIds = ( partitionIds -- intersect)
        }
      }

      res.foldLeft(Map.empty[Node, Set[PartitionedId]]) {
        case (map, (n, pIds)) =>
        {
          map + (n -> pIds.foldLeft(Set.empty[PartitionedId]) {
            case (s, pId) =>
              s ++ partitionsMap.getOrElse(pId, Set.empty[PartitionedId])
          })
        }
      }
    }

    /**
     * Returns the consistent ordered set of nodes to which messages should be routed; the order is based on the PartitionId provided.
     *
     * @param id the id based on which the order of the nodes will be determined
     * @return an ordered set of nodes
     */
    override def nextNodes(id: PartitionedId, capability: Option[Long], persistentCapability: Option[Long]): util.LinkedHashSet[Node] =
      nodesForPartition(partitionForId(id), capability, persistentCapability)
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
