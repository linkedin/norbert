package com.linkedin.norbert.network.partitioned.loadbalancer

import java.util

import com.linkedin.norbert.cluster.{InvalidClusterException, Node}
import com.linkedin.norbert.network.common.Endpoint

import scala.collection.immutable.HashSet
import scala.collection.mutable

/**
 * A partition key scheme which allows the client to specify which partition a request should go to.
 *
 * @param partitionId specifies the target partition
 * @param requestKey used for further routing within the partition (e.g. consistent hashing within the partition)
 * @tparam KeyType the type of the secondary key
 */
case class PartitionKey[KeyType](partitionId: Int, requestKey: KeyType) {

}

trait PartitionFallBackStrategy {
  def nextPartition(currentPartition: Int): Int
}

class DefaultPartitionFallbackStrategy(defaultPartition: Int) extends PartitionFallBackStrategy {
  override def nextPartition(currentPartition: Int): Int = defaultPartition
}


/**
 * A load balancer which allows the client to specify which partition a request should go to.
 *
 * The request is then routed to the load balancers of the appropriate partition.
 *
 * @param delegateLoadBalanders a map of partitionId -> load balancer delegate
 *
 * @tparam KeyType the type of the secondary key part
 */
class DirectPartitionedLoadBalancer[KeyType](delegateLoadBalanders: Map[Int, PartitionedLoadBalancer[KeyType]],
                                             fallbackStrategy: PartitionFallBackStrategy)
  extends PartitionedLoadBalancer[PartitionKey[KeyType]] {

  /**
   * Retrieve the load balancer, or fallback to a different partition, if no load balancer exists for the
   * current partition.
   *
   * If no load balancer can be found, throws an IllegalStateException
   */
  def getLoadBalancer(partitionId: Int): PartitionedLoadBalancer[KeyType] = {
    var lb = delegateLoadBalanders.get(partitionId)
    if (lb == None) {
      val nextPartitionId = fallbackStrategy.nextPartition(partitionId)
      if (nextPartitionId != partitionId) {
        // fallback once
        lb = delegateLoadBalanders.get(nextPartitionId)
      }
    }
    if (lb == None) {
      throw new IllegalStateException("No load-balancer for partition " + partitionId)
    }
    lb.get
  }

  override def nextNode(id: PartitionKey[KeyType], capability: Option[Long], persistentCapability: Option[Long]): Option[Node] =
    getLoadBalancer(id.partitionId).nextNode(id.requestKey, capability, persistentCapability)

  override def nodesForOneReplica(id: PartitionKey[KeyType], capability: Option[Long], persistentCapability: Option[Long]): Map[Node, Set[Int]] =
    getLoadBalancer(id.partitionId).nodesForOneReplica(id.requestKey, capability, persistentCapability)

  override def nodesForPartitions(id: PartitionKey[KeyType], partitions: Set[Int], capability: Option[Long], persistentCapability: Option[Long]): Map[Node, Set[Int]] =
    throw new UnsupportedOperationException("Not implemented yet")

  override def nodesForPartitionedId(id: PartitionKey[KeyType], capability: Option[Long], persistentCapability: Option[Long]): Set[Node] =
    getLoadBalancer(id.partitionId).nodesForPartitionedId(id.requestKey, capability, persistentCapability)

  override def nextNodes(id: PartitionKey[KeyType], capability: Option[Long], persistentCapability: Option[Long]): util.LinkedHashSet[Node] =
    getLoadBalancer(id.partitionId).nextNodes(id.requestKey, capability, persistentCapability)
}

class DirectPartitionedLoadBalancerFactory[KeyType](delegateFactory: PartitionedLoadBalancerFactory[KeyType],
                                                    fallbackStrategy: PartitionFallBackStrategy) extends PartitionedLoadBalancerFactory[PartitionKey[KeyType]] {

  @throws(classOf[InvalidClusterException])
  override def newLoadBalancer(nodes: Set[Endpoint]): PartitionedLoadBalancer[PartitionKey[KeyType]] = {
    val map = mutable.HashMap[Int, Set[Endpoint]]()
    nodes.foreach((e: Endpoint) => {
      e.node.partitionIds.foreach((i: Int) => {
        val setOpt = map.get(i)
        if (setOpt == None) {
          val set: Set[Endpoint] = new HashSet[Endpoint]() + e
          map += (i -> set)
        } else {
          val set: Set[Endpoint] = setOpt.get + e
          map += (i -> set)
        }
      })
    })

    // pardon my crappy scala
    val delegates : Map[Int,PartitionedLoadBalancer[KeyType]] = Map() ++ map.mapValues(v => delegateFactory.newLoadBalancer( v ))
    new DirectPartitionedLoadBalancer[KeyType](delegates, fallbackStrategy)
  }

  override def getNumPartitions(endpoints: Set[Endpoint]): Int = {
    val partitionIds = endpoints.map((x: Endpoint) => x.node.partitionIds)
    val flat = new HashSet[Int] ++ partitionIds
    flat.size
  }
}