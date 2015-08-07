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

import com.linkedin.norbert.logging.Logging
import com.linkedin.norbert.network.common.Endpoint
import com.linkedin.norbert.network.partitioned.loadbalancer.{DefaultPartitionedLoadBalancer, DefaultPartitionedLoadBalancerFactory, PartitionedLoadBalancer}
import com.linkedin.norbert.norbertutils.SystemClockComponent

/**
 * Extends DefaultPartitionedLoadBalancer to add GC-awareness.
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
abstract class GcAwarePartitionedLoadBalancerFactory[PartitionedId](numPartitions: Int, cycleTime: Int, slotTime: Int, serveRequestsIfPartitionMissing: Boolean = true)
        extends DefaultPartitionedLoadBalancerFactory[PartitionedId](numPartitions, serveRequestsIfPartitionMissing) with Logging {

  override def newLoadBalancer(endpoints: Set[Endpoint]): PartitionedLoadBalancer[PartitionedId] = GcAwarePartitionedLoadBalancer(endpoints, partitionForId, numPartitions, cycleTime, slotTime, serveRequestsIfPartitionMissing)

}

object GcAwarePartitionedLoadBalancer {

  def apply[PartitionedId](endpoints: Set[Endpoint], partitionForId: PartitionedId => Int, numPartitions: Int, cycleTime: Int, slotTime: Int, serveRequestsIfPartitionMissing: Boolean) = {

    //generatePartitionToNodeMap requires the Logging constructor to be called, and so must be evaluated lazily.
    new GcAwarePartitionedLoadBalancer[PartitionedId](endpoints, partitionForId, cycleTime, slotTime, numPartitions, serveRequestsIfPartitionMissing) {

      validateOffsets(endpoints)

      val partitionToNodeMap = generatePartitionToNodeMap(endpoints, numPartitions, serveRequestsIfPartitionMissing)

      val gcCycleTime = cycleTime
      val gcSlotTime = slotTime
    }
  }
}

abstract class GcAwarePartitionedLoadBalancer[PartitionedId](endpoints: Set[Endpoint], partitionForId: PartitionedId => Int, numPartitions: Int, cycleTime: Int, slotTime: Int, serveRequestsIfPartitionMissing: Boolean)
        extends DefaultPartitionedLoadBalancer[PartitionedId](endpoints, partitionForId, numPartitions, serveRequestsIfPartitionMissing) with GcAwarePartitionedLoadBalancerHelper with SystemClockComponent {

}
