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


import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import com.linkedin.norbert.network.client.loadbalancer.LoadBalancerHelpers

import _root_.scala.Predef._
import cluster.{InvalidClusterException, Node}
import common.Endpoint

/**
 * A <code>PartitionedLoadBalancer</code> handles calculating the next <code>Node</code> a message should be routed to
 * based on a PartitionedId.
 */
trait PartitionedLoadBalancer[PartitionedId] {
  /**
   * Returns the next <code>Node</code> a message should be routed to based on the PartitionId provided.
   *
   * @param id the id to be used to calculate partitioning information.
   *
   * @return the <code>Node</code> to route the next message to
   */
  def nextNode(id: PartitionedId, capability: Option[Long] = None, persistentCapability: Option[Long] = None): Option[Node]

  /**
   * Returns a list of nodes representing one replica of the cluster, this is used by the PartitionedNetworkClient to handle
   * broadcast to one replica
   *
   * @return the <code>Nodes</code> to broadcast the next message to a replica to
   */
  def nodesForOneReplica(id: PartitionedId, capability: Option[Long] = None, persistentCapability: Option[Long] = None): Map[Node, Set[Int]]

  /**
   * Returns a list of nodes representing all replica for this particular partitionedId
   * @return the <code>Nodes</code> to multicast the message to
   */
  def nodesForPartitionedId(id: PartitionedId, capability: Option[Long] = None, persistentCapability: Option[Long] = None): Set[Node]

  /**
   * Calculates a mapping of nodes to partitions for broadcasting a partitioned request. Optionally uses a partitioned
   * id for consistent hashing purposes
   *
   * @return the <code>Nodes</code> to broadcast the next message to a replica to
   */
  def nodesForPartitions(id: PartitionedId, partitions: Set[Int], capability: Option[Long] = None, persistentCapability: Option[Long] = None): Map[Node, Set[Int]]

  /**
   * Calculates a mapping of nodes to partitions to ensure ids belong to the same partition will be scatter to the same node
   * @param ids
   * @param capability
   * @param persistentCapability
   * @return
   */
  def nodesForPartitionedIds(ids: Set[PartitionedId], capability: Option[Long] = None, persistentCapability: Option[Long] = None): Map[Node,  Set[PartitionedId]]  =
  {
    ids.foldLeft(Map[Node, Set[PartitionedId]]().withDefaultValue(Set())) { (map, id) =>
      val node = nextNode(id, capability, persistentCapability).getOrElse(throw new NoNodesAvailableException("Unable to satisfy request, no node available for id %s".format(id)))
      map.updated(node, map(node) + id)
    }
  }

  /**
   * Calculates a mapping of nodes to partitions. The nodes should be selected from the given number of replicas.
   * Initial implementation is delegating request to maximum degree of fan-out. Implementation should override this
   * default implementation.
   *
   * @param ids set of partition ids.
   * @param numberOfReplicas number of replica
   * @param capability
   * @param persistentCapability
   * @return a map from node to partition
   */
  def nodesForPartitionedIdsInNReplicas(ids: Set[PartitionedId], numberOfReplicas: Int, capability: Option[Long] = None,
                                       persistentCapability: Option[Long] = None): Map[Node, Set[PartitionedId]] =
  {
    // Default implementation is just select nodes from all replicas.
    nodesForPartitionedIds(ids, capability, persistentCapability)
  }

  /**
   * Calculates a mapping of nodes to partitions. The nodes should be selected from the given cluster.
   * Initial implementation is delegating request to all clusters since default load balancer cannot aware cluster.
   * Note that cluster aware load balancer should override this default implementation.
   *
   * @param ids set of partition ids.
   * @param clusterId cluster id.
   * @param capability
   * @param persistentCapability
   * @return a map from node to partition
   */
  def nodesForPartitionedIdsInOneCluster(ids: Set[PartitionedId], clusterId: Int, capability: Option[Long] = None,
      persistentCapability: Option[Long] = None): Map[Node, Set[PartitionedId]] =
  {
    // Default implementation is just select nodes from all replicas.
    nodesForPartitionedIds(ids, capability, persistentCapability)
  }

}

/**
 * A factory which can generate <code>PartitionedLoadBalancer</code>s.
 */
trait PartitionedLoadBalancerFactory[PartitionedId] {
  /**
   * Create a new load balancer instance based on the currently available <code>Node</code>s.
   *
   * @param nodes the currently available <code>Node</code>s in the cluster
   *
   * @return a new <code>PartitionedLoadBalancer</code> instance
   * @throws InvalidClusterException thrown to indicate that the current cluster topology is invalid in some way and
   * it is impossible to create a <code>LoadBalancer</code>
   */
  @throws(classOf[InvalidClusterException])
  def newLoadBalancer(nodes: Set[Endpoint]): PartitionedLoadBalancer[PartitionedId]

  def getNumPartitions(endpoints: Set[Endpoint]): Int
}

/**
 * A component which provides a <code>PartitionedLoadBalancerFactory</code>.
 */
trait PartitionedLoadBalancerFactoryComponent[PartitionedId] {
  val loadBalancerFactory: PartitionedLoadBalancerFactory[PartitionedId]
}

trait PartitionedLoadBalancerHelpers extends LoadBalancerHelpers {

  /**
   * A mapping from partition id to the <code>Node</code>s which can service that partition.
   */
  protected val partitionToNodeMap: Map[Int, (IndexedSeq[Endpoint], AtomicInteger, Array[AtomicBoolean])]

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
  def generatePartitionToNodeMap(nodes: Set[Endpoint], numPartitions: Int, serveRequestsIfPartitionMissing: Boolean): Map[Int, (IndexedSeq[Endpoint], AtomicInteger, Array[AtomicBoolean])]


  /**
   * Calculates a <code>Node</code> which can service a request for the specified partition id.
   *
   * @param partitionId the id of the partition
   *
   * @return <code>Some</code> with the <code>Node</code> which can service the partition id, <code>None</code>
   * if there are no available <code>Node</code>s for the partition requested
   */
  def nodeForPartition(partitionId: Int, capability: Option[Long] = None, persistentCapability: Option[Long] = None): Option[Node]

  /** Can this endpoint be selected at this point in time? */
  def isEndpointViable(capability: Option[Long], persistentCapability: Option[Long], endpoint: Endpoint): Boolean

  /** Compensate counter to idx + count + 1, keeping in mind overflow */
  def compensateCounter(idx: Int, count:Int, counter:AtomicInteger) {
    if (idx + 1 + count <= 0) {
      // Integer overflow
      counter.set(idx + 1 - java.lang.Integer.MAX_VALUE + count)
    }
    else {
      counter.set(idx + 1 + count)
    }
  }

}
