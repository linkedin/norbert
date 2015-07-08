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
package gcaware

import com.linkedin.norbert.cluster.InvalidClusterException
import com.linkedin.norbert.network.common.Endpoint
import com.linkedin.norbert.norbertutils.ClockComponent

/**
 * A mixin trait that provides functionality to help implement a hash based, GC Aware <code>Router</code>.
 */
trait GcAwareLoadBalancerHelper extends DefaultLoadBalancerHelper {

  this: ClockComponent =>

  protected val gcCycleTime, gcSlotTime: Int

  override def isEndpointViable(capability: Option[Long], persistentCapability: Option[Long], endpoint: Endpoint): Boolean = {
    endpoint.canServeRequests &&
    endpoint.node.isCapableOf(capability, persistentCapability) &&
    isNotCurrentlyDownToGC(endpoint.node.offset.getOrElse(throw new InvalidClusterException(
              "Trying to GC-Aware load balance without an offset for node: %d".format(endpoint.node.id))))
  }

  def isNotCurrentlyDownToGC(nodeOffset: Int): Boolean = {
    val currentOffset = (clock.getCurrentTimeMilliseconds % gcCycleTime) / gcSlotTime
    currentOffset != nodeOffset
  }

  /**
   * Given the current set of node endpoints, this method generates a mapping from the time-to-GC offset to the node.
   *
   * @param nodes the current available nodes (endpoints)
   *
   * @return a <code>Map</code> of offset to the <code>Node</code>s which service that partition
   * @throws InvalidClusterException thrown if every node doesn't have an offset assigned to it
   */
  def generateOffsetToNodeMap(nodes: Set[Endpoint]): Map[Int, Set[Endpoint]] = {
    val offsetNodeTuples = List.empty[(Int, Endpoint)]

    nodes foreach { (n: Endpoint) => (n.node.offset.getOrElse(throw new InvalidClusterException("Node %d doesn't have a GC offset".format(n.node.id)))
            , n) :: offsetNodeTuples }

    offsetNodeTuples.foldLeft(Map.empty[Int, Set[Endpoint]]) {
      case (map, (offset, node)) => map + (offset -> (map.getOrElse(offset, Set.empty[Endpoint]) + node))
    }
  }

  def validateOffsets(nodes: Set[Endpoint]): Unit = {
    nodes foreach {(n: Endpoint) => if (n.node.offset.isEmpty) throw new InvalidClusterException("Node %d doesn't have a GC offset".format(n.node.id))}
  }
}
