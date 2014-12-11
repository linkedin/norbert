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
package com.linkedin.norbert.javacompat.network;

import com.linkedin.norbert.javacompat.cluster.Node;

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * A <code>PartitionedLoadBalancer</code> handles calculating the next <code>Node</code> a message should be routed to
 * based on a PartitionedId.
 *
 * Each of these API have two versions, one without capability long, one with it. The capability long is a piece of data
 * held by Norbert node representing 64 different capability by setting each bit of the Long. If the API takes a capability
 * parameter, load balancer will only return nodes satisfying the at least the capability requested.
 */
public interface PartitionedLoadBalancer<PartitionedId> {
  /**
   * Returns the next <code>Node</code> a message should be routed to based on the PartitionId provided.
   *
   * @param id the id to be used to calculate partitioning information.
   *
   * @return the <code>Node</code> to route the next message to
   */
  Node nextNode(PartitionedId id);

   /**
   * Returns the next <code>Node</code> a message should be routed to based on the PartitionId provided.
   *
   * @param id the id to be used to calculate partitioning information.
   * @param capability the minimal capability required by client
   *
   * @return the <code>Node</code> to route the next message to
   */
  Node nextNode(PartitionedId id, Long capability);

  /**
   * Returns the next <code>Node</code> a message should be routed to based on the PartitionId provided.
   *
   * @param id the id to be used to calculate partitioning information.
   * @param capability the minimal capability required by client
   * @param persistentCapability the capability of more persistent nature
   *
   * @return the <code>Node</code> to route the next message to
   */
  Node nextNode(PartitionedId id, Long capability, Long persistentCapability);


    /**
     * Returns the next <code>Node</code> a message should be routed to based on the PartitionId provided.
     *
     * @param id the id to be used to calculate partitioning information.
     *
     * @return the <code>Node</code> to route the next message to
     */
    LinkedHashSet<Node> nextNodes(PartitionedId id);

    /**
     * Returns the next <code>Node</code> a message should be routed to based on the PartitionId provided.
     *
     * @param id the id to be used to calculate partitioning information.
     * @param capability the minimal capability required by client
     *
     * @return the <code>Node</code> to route the next message to
     */
    LinkedHashSet<Node> nextNodes(PartitionedId id, Long capability);

    /**
     * Returns the next <code>Node</code> a message should be routed to based on the PartitionId provided.
     *
     * @param id the id to be used to calculate partitioning information.
     * @param capability the minimal capability required by client
     * @param persistentCapability the capability of more persistent nature
     *
     * @return the <code>Node</code> to route the next message to
     */
    LinkedHashSet<Node> nextNodes(PartitionedId id, Long capability, Long persistentCapability);


    /**
   * Returns all replica nodes for the same partitionedId
   * @return the <code>Nodes</code> to multicast the next messages to each replica
   */
  Set<Node> nodesForPartitionedId(PartitionedId id);

  /**
   * Returns all replica nodes for the same partitionedId
   * @return the <code>Nodes</code> to multicast the next messages to each replica
   */
  Set<Node> nodesForPartitionedId(PartitionedId id, Long capability);

  /**
   * Returns all replica nodes for the same partitionedId
   * @return the <code>Nodes</code> to multicast the next messages to each replica
   */
  Set<Node> nodesForPartitionedId(PartitionedId id, Long capability, Long persistentCapability);

  /**
   * Returns a set of nodes represents one replica of the cluster, this is used by the PartitionedNetworkClient to handle
   * broadcast to one replica
   *
   * @return the set of <code>Nodes</code> to broadcast the next message to a replica to
   */
  Map<Node, Set<Integer>> nodesForOneReplica(PartitionedId id);

  /**
   * Returns a set of nodes represents one replica of the cluster, this is used by the PartitionedNetworkClient to handle
   * broadcast to one replica
   *
   * @return the set of <code>Nodes</code> to broadcast the next message to a replica to
   */
  Map<Node, Set<Integer>> nodesForOneReplica(PartitionedId id, Long capability);

  /**
   * Returns a set of nodes represents one replica of the cluster, this is used by the PartitionedNetworkClient to handle
   * broadcast to one replica
   *
   * @return the set of <code>Nodes</code> to broadcast the next message to a replica to
   */
  Map<Node, Set<Integer>> nodesForOneReplica(PartitionedId id, Long capability, Long persistentCapability);

  /**
   * Calculates a mapping of nodes to partitions for broadcasting a partitioned request. Optionally uses a partitioned
   * id for consistent hashing purposes
   *
   * @return the <code>Nodes</code> to broadcast the next message to a replica to
   */
  Map<Node, Set<Integer>> nodesForPartitions(PartitionedId id, Set<Integer> partitions);

  /**
   * Calculates a mapping of nodes to partitions for broadcasting a partitioned request. Optionally uses a partitioned
   * id for consistent hashing purposes
   *
   * @return the <code>Nodes</code> to broadcast the next message to a replica to
   */
  Map<Node, Set<Integer>> nodesForPartitions(PartitionedId id, Set<Integer> partitions, Long capability);

  /**
   * Calculates a mapping of nodes to partitions for broadcasting a partitioned request. Optionally uses a partitioned
   * id for consistent hashing purposes
   *
   * @return the <code>Nodes</code> to broadcast the next message to a replica to
   */
  Map<Node, Set<Integer>> nodesForPartitions(PartitionedId id, Set<Integer> partitions, Long capability, Long persistentCapability);
}
