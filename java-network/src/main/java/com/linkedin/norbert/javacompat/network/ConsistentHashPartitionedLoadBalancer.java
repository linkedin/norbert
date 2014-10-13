package com.linkedin.norbert.javacompat.network;

import com.linkedin.norbert.javacompat.cluster.Node;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;



public class ConsistentHashPartitionedLoadBalancer<PartitionedId> implements PartitionedLoadBalancer<PartitionedId>
{
  private final HashFunction<String> _hashFunction;
  private final Map<Integer, NavigableMap<Long, Endpoint>> _rings;
  private final TreeMap<Long, Map<Endpoint, Set<Integer>>> _routingMap;

  public ConsistentHashPartitionedLoadBalancer(HashFunction<String> hashFunction,
                                               Map<Integer, NavigableMap<Long, Endpoint>> rings,
                                               TreeMap<Long, Map<Endpoint, Set<Integer>>> routingMap,
                                               PartitionedLoadBalancer<PartitionedId> fallThrough) {
    this._hashFunction = hashFunction;
    this._rings = rings;
    this._routingMap = routingMap;
  }

  public static <PartitionedId> ConsistentHashPartitionedLoadBalancer<PartitionedId> build(int bucketCount,
                                                            HashFunction<String> hashFunction,
                                                            Set<Endpoint> endpoints,
                                                            PartitionedLoadBalancer<PartitionedId> fallThrough) {

    // Gather set of nodes for each partition
    Map<Integer, Set<Endpoint>> partitionNodes = new TreeMap<Integer, Set<Endpoint>>();
    for (Endpoint endpoint : endpoints)
    {
      Node node = endpoint.getNode();
      for (Integer partId : node.getPartitionIds())
      {
        Set<Endpoint> partNodes = partitionNodes.get(partId);
        if (partNodes == null)
        {
          partNodes = new HashSet<Endpoint>();
          partitionNodes.put(partId, partNodes);
        }
        partNodes.add(endpoint);
      }
    }

    // Builds individual ring for each partitions
    int maxSize = 0;
    Map<Integer, NavigableMap<Long, Endpoint>> rings = new TreeMap<Integer, NavigableMap<Long, Endpoint>>();
    for (Map.Entry<Integer, Set<Endpoint>> entry : partitionNodes.entrySet())
    {
      Integer partId = entry.getKey();
      NavigableMap<Long, Endpoint> ring = new TreeMap<Long, Endpoint>();
      if (maxSize < entry.getValue().size())
      {
        maxSize = entry.getValue().size();
      }

      for (Endpoint endpoint : entry.getValue())
      {
        for (int i = 0; i < bucketCount; i++)
        {
          // Use node-[node_id]-[bucket_id] as key
          // Hence for the same node, same bucket id will always hash to the same place
          // This helps to maintain consistency when the bucketCount changed
          ring.put(hashFunction.hash(String.format("node-%d-%d", endpoint.getNode().getId(), i)), endpoint);
        }
      }

      rings.put(partId, ring);
    }

    // Build one final ring.

    TreeMap<Long, Map<Endpoint, Set<Integer>>> routingMap = new TreeMap<Long, Map<Endpoint, Set<Integer>>>();

    for (int slot = 0; slot < bucketCount * maxSize; slot++)
    {
      Long point = hashFunction.hash(String.format("ring-%d", slot));

      // For each generated point on the ring, gather node for each partition.
      Map<Endpoint, Set<Integer>> pointRoute = new HashMap<Endpoint, Set<Integer>>();
      for (Map.Entry<Integer, NavigableMap<Long, Endpoint>> ringEntry : rings.entrySet())
      {
        Integer partitionId = ringEntry.getKey();
        NavigableMap<Long, Endpoint> ring = ringEntry.getValue();
        Endpoint endpoint = lookup(ring, point).getValue();

        Set<Integer> partitionSet = pointRoute.get(endpoint);
        if (partitionSet == null)
        {
          partitionSet = new HashSet<Integer>();
        }
        partitionSet.add(partitionId); // Add partition to the node
        pointRoute.put(endpoint, partitionSet);
      }
      routingMap.put(point, pointRoute);
    }

    return new ConsistentHashPartitionedLoadBalancer<PartitionedId>(hashFunction, rings, routingMap, fallThrough);
  }

  @Override 
  public Node nextNode(PartitionedId partitionedId, Long capability)
  {
    return nextNode(partitionedId, capability, 0L);
  }

  @Override
  public Node nextNode(PartitionedId partitionedId)
  {
    return nextNode(partitionedId, 0L, 0L);
  }
  
  @Override 
  public Node nextNode(PartitionedId partitionedId, Long capability, Long persistentCapability)
  {
    long hash = _hashFunction.hash(partitionedId.toString());
    int partitionId = (int)(Math.abs(hash) % _rings.size());
    NavigableMap<Long, Endpoint> ring = _rings.get(partitionId);
    Endpoint endpoint = searchWheel(ring, hash, new Function<Endpoint, Boolean>() {
      @Override
      public Boolean apply(Endpoint key) {
        return key.canServeRequests();
      }
    });
    return endpoint.getNode();
  }

  @Override
  public Set<Node> nodesForPartitionedId(PartitionedId partitionedId) {
    return nodesForPartitionedId(partitionedId, 0L, 0L);
  }

  @Override
  public Set<Node> nodesForPartitionedId(PartitionedId partitionedId, Long capability) {
    return nodesForPartitionedId(partitionedId, capability, 0L);
  }

  @Override
  public Set<Node> nodesForPartitionedId(PartitionedId partitionedId, Long capability, Long persistentCapability) {
    return nodesForOneReplica(partitionedId, capability, persistentCapability).keySet();
  }

  @Override
  public Map<Node, Set<Integer>> nodesForOneReplica(PartitionedId partitionedId)
  {
    return nodesForOneReplica(partitionedId, 0L, 0L);
  }

  @Override
  public Map<Node, Set<Integer>> nodesForOneReplica(PartitionedId partitionedId, Long capability) {
    return nodesForOneReplica(partitionedId, capability, 0L);
  }

  @Override
  public Map<Node, Set<Integer>> nodesForOneReplica(PartitionedId partitionedId, Long capability, Long persistentCapability) {
    Map<Endpoint, Set<Integer>> replica = lookup(_routingMap, _hashFunction.hash(partitionedId.toString())).getValue();
    Map<Node, Set<Integer>> results = new HashMap<Node, Set<Integer>>();

    Set<Integer> unsatisfiedPartitions = new HashSet<Integer>();

    // Attempt to filter out results that are not available
    for(Map.Entry<Endpoint, Set<Integer>> entry : replica.entrySet())
    {

      Node node = entry.getKey().getNode();
      Set<Integer> partitionsToServe = entry.getValue();

      if(entry.getKey().canServeRequests())
      {
        results.put(node, new HashSet<Integer>(partitionsToServe));
      }
      else
      {
        unsatisfiedPartitions.addAll(partitionsToServe);
      }
    }


    if(unsatisfiedPartitions.size() > 0)
    {
      Map<Node, Set<Integer>> resolved = nodesForPartitions(partitionedId, unsatisfiedPartitions);
      for(Map.Entry<Node, Set<Integer>> entry : resolved.entrySet())
      {
        Set<Integer> partitions = results.get(entry.getKey());
        if(partitions != null)
        {
          partitions.addAll(entry.getValue());
        }
        else
        {
          results.put(entry.getKey(), entry.getValue());
        }
      }
    }

    return results;
  }

  @Override
  public Map<Node, Set<Integer>> nodesForPartitions(PartitionedId partitionedId, Set<Integer> partitions) {
    return nodesForPartitions(partitionedId, partitions, 0L, 0L);
  }

  @Override
  public Map<Node, Set<Integer>> nodesForPartitions(PartitionedId partitionedId, Set<Integer> partitions, Long capability) {
    return nodesForPartitions(partitionedId, partitions, capability, 0L);
  }

  @Override
  public Map<Node, Set<Integer>> nodesForPartitions(PartitionedId partitionedId, Set<Integer> partitions, Long capability, Long persistentCapability) {
    Map<Node, Set<Integer>> nodes = new HashMap<Node, Set<Integer>>();
    for(int partition : partitions)
    {
      NavigableMap<Long, Endpoint> ring = _rings.get(partition);
      Endpoint endpoint = searchWheel(ring, _hashFunction.hash(partitionedId.toString()), new Function<Endpoint, Boolean>() {
        @Override
        public Boolean apply(Endpoint key) {
          return key.canServeRequests();
        }
      });

      Node node = endpoint.getNode();
      Set<Integer> partitionsForNode = nodes.get(node);
      if(partitionsForNode == null)
        partitionsForNode = new HashSet<Integer>();

      partitionsForNode.add(partition);
      nodes.put(node, partitionsForNode);
    }

    return nodes;
  }

  public static interface Function<K, V> {
    public V apply(K key);
  }

  private static <K, V> V searchWheel(NavigableMap<K, V> ring, K key, Function<V, Boolean> predicate)
  {
    if(ring.isEmpty())
      return null;

    final Map.Entry<K, V> original = lookup(ring, key);
    Map.Entry<K, V> candidate = original;
    do {
      if(predicate.apply(candidate.getValue()))
        return candidate.getValue();

      candidate = rotateWheel(ring, candidate.getKey());
    } while(candidate != original);

    return candidate.getValue();
  }

  private static <K, V> Map.Entry<K, V> rotateWheel(NavigableMap<K, V> ring, K key)
  {
    Map.Entry<K, V> nextEntry = ring.higherEntry(key);
    if(nextEntry == null)
      return ring.firstEntry();
    return nextEntry;
  }

  private static <K, V> Map.Entry<K, V> lookup(NavigableMap<K, V> ring, K key)
  {
    final V result = ring.get(key);
    if (result == null)
    {       // Not a direct match
      Map.Entry<K, V> entry = ring.ceilingEntry(key);
      if(entry == null)
        return ring.firstEntry();
      else
        return entry;
    } else {
      return new AbstractMap.SimpleEntry<K, V>(key, result);
    }
  }
}
