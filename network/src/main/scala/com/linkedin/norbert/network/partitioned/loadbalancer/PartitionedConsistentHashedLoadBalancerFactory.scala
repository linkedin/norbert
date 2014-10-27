package com.linkedin.norbert.network.partitioned.loadbalancer

import java.util
import java.util.TreeMap
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import com.linkedin.norbert.cluster.{InvalidClusterException, Node}
import com.linkedin.norbert.network.common.Endpoint

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

/**
 * Similar to both the DefaultPartitionedLoadBalancer and  the SimpleConsistentHashedLoadBalancer, but uses a wheel per partition.
 * This means that if you shard your data according to some key but maintain replicas, rather than load balancing between the replicas,
 * the replicas will also use consistent hashing. This can be useful for a distributed database like system where you'd like
 * to shard the data by key, but you'd also like to maximize cache utilization for the replicas.
 */
class PartitionedConsistentHashedLoadBalancerFactory[PartitionedId](numPartitions: Int,
                                                                    numReplicas: Int,
                                                                    hashFn: PartitionedId => Int,
                                                                    endpointHashFn: String => Int,
                                                                    serveRequestsIfPartitionMissing: Boolean)
  extends DefaultPartitionedLoadBalancerFactory[PartitionedId](numPartitions, serveRequestsIfPartitionMissing) {

  def this(slicesPerEndpoint: Int, hashFn: PartitionedId => Int, endpointHashFn: String => Int, serveRequestsIfPartitionMissing: Boolean) = {
    this(-1, slicesPerEndpoint, hashFn, endpointHashFn, serveRequestsIfPartitionMissing)
  }

  def getNumPartitions(endpoints: Set[Endpoint]) = {
    if (numPartitions == -1) {
      endpoints.flatMap(_.node.partitionIds).size
    } else {
      numPartitions
    }
  }

  protected def calculateHash(id: PartitionedId) = hashFn(id)

  @throws(classOf[InvalidClusterException])
  override def newLoadBalancer(endpoints: Set[Endpoint]): PartitionedConsistentHashedLoadBalancer[PartitionedId] = {
    val partitions = endpoints.foldLeft(Map.empty[Int, Set[Endpoint]]) { (map, endpoint) =>
      endpoint.node.partitionIds.foldLeft(map) { (map, partition) =>
        map + (partition -> (map.getOrElse(partition, Set.empty[Endpoint]) + endpoint))
      }
    }

    val wheels = partitions.map { case (partition, endpointsForPartition) =>
      val wheel = new TreeMap[Int, Endpoint]
      endpointsForPartition.foreach { endpoint =>
        var r = 0
        while (r < numReplicas) {
          val node = endpoint.node
          val distKey = node.id + ":" + partition + ":" + r + ":" + node.url
          wheel.put(endpointHashFn(distKey), endpoint)
          r += 1
        }
      }

      (partition, wheel)
    }

    val nPartitions = if(this.numPartitions == -1) endpoints.flatMap(_.node.partitionIds).size else numPartitions
    new PartitionedConsistentHashedLoadBalancer(nPartitions, wheels, hashFn, serveRequestsIfPartitionMissing)
  }
}

class PartitionedConsistentHashedLoadBalancer[PartitionedId](numPartitions: Int, wheels: Map[Int, TreeMap[Int, Endpoint]], hashFn: PartitionedId => Int, serveRequestsIfPartitionMissing: Boolean = true)
        extends PartitionedLoadBalancer[PartitionedId] with DefaultLoadBalancerHelper {
  import scala.collection.JavaConversions._
  val endpoints = wheels.values.flatMap(_.values).toSet
  val partitionToNodeMap = generatePartitionToNodeMap(endpoints, numPartitions, serveRequestsIfPartitionMissing)
  val partitionIds = wheels.keySet.toSet

  def nodesForOneReplica(id: PartitionedId, capability: Option[Long] = None, persistentCapability: Option[Long] = None) = {
    nodesForPartitions(id, wheels, capability, persistentCapability)
  }

  def nodesForPartitionedId(id: PartitionedId, capability: Option[Long] = None, persistentCapability: Option[Long] = None) = {
    val hash = hashFn(id)
    val partitionId = hash.abs % numPartitions
    wheels.get(partitionId).flatMap { wheel => Option(wheel.foldLeft(Set.empty[Node]) { case (set, (p, e)) => if (e.node.isCapableOf(capability, persistentCapability)) set + e.node else set }) }.get
  }

  def nodesForPartitions(id: PartitionedId, partitions: Set[Int], capability: Option[Long] = None, persistentCapability: Option[Long] = None) = {
    nodesForPartitions(id, wheels.filterKeys(partitions contains _), capability, persistentCapability)
  }
  
  private def nodesForPartitions(id: PartitionedId, wheels: Map[Int, TreeMap[Int, Endpoint]], capability: Option[Long], persistentCapability: Option[Long]) = {
    if (id == null) {
      nodesForPartitions0(partitionToNodeMap filterKeys wheels.containsKey, capability, persistentCapability)
    } else {
      val hash = hashFn(id)

      wheels.foldLeft(Map.empty[Node, Set[Int]]) { case (accumulator, (partitionId, wheel)) =>
        val endpoint = PartitionUtil.searchWheel(wheel, hash, (e: Endpoint) => e.canServeRequests && e.node.isCapableOf(capability, persistentCapability) )

        if(endpoint.isDefined) {
          val node = endpoint.get.node
          val partitions = accumulator.getOrElse(node, Set.empty[Int]) + partitionId
          accumulator + (node -> partitions)
        } else if(serveRequestsIfPartitionMissing) {

          log.warn("All nodes appear to be unresponsive for partition %s, selecting the original node."
            .format(partitionId))

          val originalEndpoint = PartitionUtil.searchWheel(wheel, hash, (e: Endpoint) => e.node.isCapableOf(capability, persistentCapability))
          val node = originalEndpoint.get.node
          val partitions = accumulator.getOrElse(node, Set.empty[Int]) + partitionId
          accumulator + (node -> partitions)

        } else
            throw new InvalidClusterException("Partition %s is unavailable, cannot serve requests.".format(partitionId))
      }
    }
  }  
  
  private def nodesForPartitions0(partitionToNodeMap: Map[Int, (IndexedSeq[Endpoint], AtomicInteger, Array[AtomicBoolean])], capability: Option[Long], persistentCapability: Option[Long] = None) = {
    partitionToNodeMap.keys.foldLeft(Map.empty[Node, Set[Int]]) { (map, partition) =>
      val nodeOption = nodeForPartition(partition, capability, persistentCapability)
      if(nodeOption isDefined) {
        val n = nodeOption.get
        map + (n -> (map.getOrElse(n, Set.empty[Int]) + partition))
      } else if(serveRequestsIfPartitionMissing) {
        log.warn("Partition %s is unavailable, attempting to continue serving requests to other partitions.".format(partition))
        map
      } else
        throw new InvalidClusterException("Partition %s is unavailable, cannot serve requests.".format(partition))
    }
  }

  override def nextNodes(id: PartitionedId, capability: Option[Long] = None, persistentCapability: Option[Long] = None): util.LinkedHashSet[Node] = {
    val hash = hashFn(id)
    val partitionId = hash.abs % numPartitions
    // FIXME: wheels.get(partitionId) here may fail, we should use PartitionUtil.searchWheel to search the outer wheel as well
    // this can happen if numPartitions is a lie, or if we have non-contiguous partitions (0, 2, 4), form
    val innerMapOpt = wheels.get(partitionId)
    val result = new util.LinkedHashSet[Node]()
    if (innerMapOpt.isDefined) {
      val innerMap = innerMapOpt.get
      val startEntry = PartitionUtil.wheelEntry(innerMap, hash)
      if (startEntry != null) {
        result.add(startEntry.getValue.node)
        var nextEntry = PartitionUtil.rotateWheel(innerMap, startEntry.getKey)
        while (nextEntry != startEntry) {
          result.add(nextEntry.getValue.node)
          nextEntry = PartitionUtil.rotateWheel(innerMap, nextEntry.getKey)
        }
        return result
      }
    }
    log.warn("Failed to find mapping for %s, expect routing failures".format(id))
    result
  }

  def nextNode(id: PartitionedId, capability: Option[Long] = None, persistentCapability: Option[Long] = None): Option[Node] = {
    val hash = hashFn(id)
    val partitionId = hash.abs % numPartitions

    // FIXME: wheels.get(partitionId) here may fail, we should use PartitionUtil.searchWheel to search the outer wheel as well
    wheels.get(partitionId).flatMap { wheel =>
      PartitionUtil.searchWheel(wheel, hash, (e: Endpoint) => e.canServeRequests && e.node.isCapableOf(capability, persistentCapability) )
    }.map(_.node)
  }

  def partitionForId(id: PartitionedId): Int = {
    hashFn(id).abs % numPartitions
  }
}
