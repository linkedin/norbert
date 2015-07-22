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
package com.linkedin.norbert
package network
package partitioned
package loadbalancer

import cluster.{InvalidClusterException, Node}
import com.linkedin.norbert.network.util.ConcurrentCyclicCounter
import common.Endpoint
import java.util.concurrent.atomic.AtomicBoolean
import logging.Logging

/**
 * A mixin trait that provides functionality to help implement a hash based <code>Router</code>.
 */
trait DefaultPartitionedLoadBalancerHelper extends PartitionedLoadBalancerHelpers with Logging {

  /**
   * Given the currently available <code>Node</code>s and the total number of partitions in the cluster, this method
   * generates a <code>Map</code> of partition id to the <code>Node</code>s which service that partition.
   *
   * @param nodes the current available nodes
   * @param numPartitions the total number of partitions in the cluster
   *
   * @return a <code>Map</code> of partition id to the <code>Node</code>s which service that partition
   * @throws InvalidClusterException thrown if every partition doesn't have at least one available <code>Node</code>
   * assigned to it
   */
  def generatePartitionToNodeMap(nodes: Set[Endpoint], numPartitions: Int, serveRequestsIfPartitionMissing: Boolean): Map[Int, (IndexedSeq[Endpoint], ConcurrentCyclicCounter, Array[AtomicBoolean])] = {
    val partitionToNodeMap = (for (n <- nodes; p <- n.node.partitionIds) yield(p, n)).foldLeft(Map.empty[Int, IndexedSeq[Endpoint]]) {
      case (map, (partitionId, node)) => map + (partitionId -> (node +: map.get(partitionId).getOrElse(Vector.empty[Endpoint])))
    }

    val possiblePartitions = (0 until numPartitions).toSet
    val missingPartitions = possiblePartitions diff (partitionToNodeMap.keys.toSet)

    if(missingPartitions.size == possiblePartitions.size)
      throw new InvalidClusterException("Every single partition appears to be missing. There are %d partitions".format(numPartitions))
    else if(!missingPartitions.isEmpty) {
      if(serveRequestsIfPartitionMissing)
        log.warn("Partitions %s are unavailable, attempting to continue serving requests to other partitions.".format(missingPartitions))
      else
        throw new InvalidClusterException("Partitions %s are unavailable, cannot serve requests.".format(missingPartitions))
    }


    partitionToNodeMap.map { case (pId, endPoints) =>
      val states = new Array[AtomicBoolean](endPoints.size)
      (0 to endPoints.size -1).foreach(states(_) = new AtomicBoolean(true))
      pId -> (endPoints, new ConcurrentCyclicCounter, states)
    }
  }

  /**
   * Calculates a <code>Node</code> which can service a request for the specified partition id.
   *
   * @param partitionId the id of the partition
   *
   * @return <code>Some</code> with the <code>Node</code> which can service the partition id, <code>None</code>
   * if there are no available <code>Node</code>s for the partition requested
   */
  def nodeForPartition(partitionId: Int, capability: Option[Long] = None, persistentCapability: Option[Long] = None): Option[Node] = {
    partitionToNodeMap.get(partitionId) match {
      case None =>
        None
      case Some((endpoints, counter, states)) =>
        val es = endpoints.size
        val idx = counter.getAndIncrement
        var i = idx
        var loopCount = 0
        do {
          val endpoint = endpoints(i % es)
          if(isEndpointViable(capability, persistentCapability, endpoint)) {
            counter.compensate(idx, loopCount)
            return Some(endpoint.node)
          }

          i = i + 1
          if (i < 0) i = 0
          loopCount = loopCount + 1
        } while (loopCount < es)

        // To REVIEW: I don't think the compensate counter below is needed. The counter is already incremented when get
        //            is called the first time, and if no node was found (which is the case when the code below is hit),
        //            that behaviour should suffice. The below would do the same mod es, and this way we prevent the counter
        //            hurrying its way to MAX_VALUE. Additionally, I changed the while loop above to < es from <= es, as
        //            this does one redundant cycle (es + 1 times instead of es times).

//        compensateCounter(idx, loopCount, counter)
        nodeToReturnWhenNothingViableFound(endpoints, idx)
    }
  }

  // Returns the next node in round-robin fashion. Takes in the list of endpoints and the original counter value for that
  // set of endpoints
  def nodeToReturnWhenNothingViableFound(endpoints: IndexedSeq[Endpoint], idx: Int): Some[Node] = {
    val es = endpoints.size
    Some(endpoints(idx % es).node)
  }
}
