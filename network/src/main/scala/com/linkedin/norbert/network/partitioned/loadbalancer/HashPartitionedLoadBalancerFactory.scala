package com.linkedin.norbert.network.partitioned.loadbalancer


/**
 * A very simple adapter of a load balancer factory that provides a default hashCode based implementation
 * for hash calculations.
 *
 * Suitable for use within Java classes.
 *
 * @see com.linkedin.norbert.javacompat.network.ScalaLbfToJavaLbf
 */
class HashPartitionedLoadBalancerFactory[PartitionedId](numPartitions: Int,
                                                        numReplicas: Int,
                                                        hashFn: PartitionedId => Int,
                                                        endpointHashFn: String => Int,
                                                        serveRequestsIfPartitionMissing: Boolean)
  extends PartitionedConsistentHashedLoadBalancerFactory[PartitionedId](
    numPartitions: Int,
    numReplicas: Int,
    hashFn: PartitionedId => Int,
    endpointHashFn: String => Int,
    serveRequestsIfPartitionMissing: Boolean) {
  def this( numReplicas: Int) = {
    this(-1, numReplicas, (p: PartitionedId) => p.hashCode, (p: String) => p.hashCode, true)
  }

  def this( numPartitions: Int, numReplicas: Int) = {
    this(numPartitions, numReplicas, (p: PartitionedId) => p.hashCode, (p: String) => p.hashCode, true)
  }
}
