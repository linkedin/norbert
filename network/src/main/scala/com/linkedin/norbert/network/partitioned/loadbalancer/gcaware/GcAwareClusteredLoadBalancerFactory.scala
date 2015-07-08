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

import com.linkedin.norbert.cluster.Node
import com.linkedin.norbert.logging.Logging
import com.linkedin.norbert.network.common.Endpoint
import com.linkedin.norbert.network.partitioned.loadbalancer.{DefaultClusteredLoadBalancer, DefaultClusteredLoadBalancerFactory, PartitionedLoadBalancer}
import com.linkedin.norbert.norbertutils.SystemClockComponent

/**
 * Extends DefaultClusteredLoadBalancer to add GC-awareness.
 * Nodes are additionally filtered to exclude those that are currently garbage collecting.
 *
 * @param cycleTime: The time period in which each node in the colo undergoes garbage collection exactly once
 * @param slotTime: The time allotted for the nodes in one cluster to finish pending requests and garbage collect.
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
        extends DefaultClusteredLoadBalancer[PartitionedId](endpoints, partitionForId, clusterId, numPartitions, serveRequestsIfPartitionMissing) with GcAwareLoadBalancerHelper with SystemClockComponent {

}