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
import org.apache.log4j.Logger;

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * This class will logically create a circle of numReplicas * numPartition divisions.
 */
public class RingHashPartitionedLoadBalancer implements PartitionedLoadBalancer<Integer> {
  public static final Logger log = Logger.getLogger(RingHashPartitionedLoadBalancer.class);

  private final TreeMap<Long, Endpoint> nodeCircleMap = new TreeMap<Long, Endpoint>();
  private final HashFunction<String> hashStrategy;

  RingHashPartitionedLoadBalancer(int numReplicas, Set<Endpoint> nodes, HashFunction<String> hashingStrategy) {
    hashStrategy = hashingStrategy;
    for (Endpoint endpoint : nodes) {
      Node node = endpoint.getNode();
      Set<Integer> partitionedIds = node.getPartitionIds();
      for (Integer partitionId : partitionedIds) {
        for (int r = 0; r < numReplicas; r++) {
          String distKey = node.getId() + ":" + partitionId + ":" + r + ":" + node.getUrl();
          nodeCircleMap.put(hashingStrategy.hash(distKey), endpoint);
        }
      }
    }
  }

  public Node nextNode(Integer partitionedId) {
    return nextNode(partitionedId, 0L, 0L);
  }

  public Node nextNode(Integer partitionedId, Long capability) {
    return nextNode(partitionedId, capability, 0L);
  }

  @Override
  public Node nextNode(Integer partitionedId, Long capability, Long persistentCapability) {
    if (nodeCircleMap.isEmpty())
      return null;

    Long hash = hashStrategy.hash(partitionedId.toString());
    hash = nodeCircleMap.ceilingKey(hash);
    hash = (hash == null) ? nodeCircleMap.firstKey() : hash;

    Endpoint firstEndpoint = nodeCircleMap.get(hash);
    Endpoint endpoint = firstEndpoint;

    do {
      Node node = endpoint.getNode();
      if(endpoint.canServeRequests() && node.isCapableOf(capability, persistentCapability)) {
        if (log.isDebugEnabled())
          log.debug(partitionedId + " is sent to node " + node.getId());
        return node;
      } else {
        Map.Entry<Long, Endpoint> nextEntry = nodeCircleMap.higherEntry(hash);
        nextEntry = (nextEntry == null) ? nodeCircleMap.firstEntry() : nextEntry;

        hash = nextEntry.getKey();
        endpoint = nextEntry.getValue();
      }
    } while(endpoint != firstEndpoint);

    log.warn("All endpoints seem unavailable! Using the default");
    return firstEndpoint.getNode();
  }

    @Override
    public LinkedHashSet<Node> nextNodes(Integer integer) {
        return null;
    }

    @Override
    public LinkedHashSet<Node> nextNodes(Integer integer, Long capability) {
        return null;
    }

    @Override
    public LinkedHashSet<Node> nextNodes(Integer integer, Long capability, Long persistentCapability) {
        return null;
    }

    @Override
  public Set<Node> nodesForPartitionedId(Integer partitionedId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Set<Node> nodesForPartitionedId(Integer partitionedId, Long capability) {
    throw new UnsupportedOperationException();
  }
  
  @Override
  public Set<Node> nodesForPartitionedId(Integer partitionedId, Long capability, Long persistentCapability) {
    throw new UnsupportedOperationException();
  }
  
  @Override
  public Map<Node, Set<Integer>> nodesForOneReplica(Integer partitionedId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Map<Node, Set<Integer>> nodesForOneReplica(Integer partitionedId, Long capability ) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Map<Node, Set<Integer>> nodesForOneReplica(Integer partitionedId, Long capability, Long persistentCapability ) {
    throw new UnsupportedOperationException();
  }
  
  @Override
  public Map<Node, Set<Integer>> nodesForPartitions(Integer integer, Set<Integer> partitions) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Map<Node, Set<Integer>> nodesForPartitions(Integer integer, Set<Integer> partitions, Long capability) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Map<Node, Set<Integer>> nodesForPartitions(Integer integer, Set<Integer> partitions, Long capability, Long persistentCapability) {
    throw new UnsupportedOperationException();
  }
}
