package com.linkedin.norbert
package javacompat
package network

import java.{lang, util}

import com.linkedin.norbert.EndpointConversions._
import com.linkedin.norbert.javacompat._
import com.linkedin.norbert.javacompat.cluster.{JavaNode, Node => JNode}
import com.linkedin.norbert.network.partitioned.loadbalancer.{PartitionedLoadBalancerFactory => SPartitionedLoadBalancerFactory}


class ScalaLbfToJavaLbf[PartitionedId](scalaLbf: SPartitionedLoadBalancerFactory[PartitionedId]) extends PartitionedLoadBalancerFactory[PartitionedId] {

  def newLoadBalancer(endpoints: java.util.Set[Endpoint]) = {
    val scalaBalancer = scalaLbf.newLoadBalancer(endpoints)

    new PartitionedLoadBalancer[PartitionedId] {
      def nodesForOneReplica(id: PartitionedId) = nodesForOneReplica(id, 0L, 0L)

      def nodesForOneReplica(id: PartitionedId, capability: java.lang.Long) = {
	nodesForOneReplica(id, capability, 0L)
      }

      def nodesForOneReplica(id: PartitionedId, capability: java.lang.Long, persistentCapability: java.lang.Long) = {
        val replica = scalaBalancer.nodesForOneReplica(id, capability, persistentCapability)
        val result = new java.util.HashMap[JNode, java.util.Set[java.lang.Integer]](replica.size)

        replica.foreach { case (node, partitions) =>
          result.put(node, partitions)
                        }

        result        
      }

      def nextNode(id: PartitionedId) = nextNode(id, 0L, 0L)

      def nextNode(id: PartitionedId, capability: java.lang.Long) = nextNode(id, capability, 0L)

      def nextNode(id: PartitionedId, capability: java.lang.Long, persistentCapability: java.lang.Long) =  {
        scalaBalancer.nextNode(id, capability, persistentCapability) match {
          case Some(n) =>n
          case None => null
        }
      }

      def nodesForPartitionedId(id: PartitionedId) = nodesForPartitionedId(id, 0L, 0L)

      def nodesForPartitionedId(id: PartitionedId, capability: java.lang.Long) = nodesForPartitionedId(id, capability, 0L)

      def nodesForPartitionedId(id: PartitionedId, capability: java.lang.Long, persistentCapability: java.lang.Long) = {
        val set = scalaBalancer.nodesForPartitionedId(id, capability, persistentCapability)
        val jSet = new java.util.HashSet[JNode]()
        set.foldLeft(jSet) { case (jSet, node) => {jSet.add(node); jSet} }
        jSet
      }

      def nodesForPartitions(id: PartitionedId, partitions: java.util.Set[java.lang.Integer]) = nodesForPartitions(id, partitions, 0L, 0L)

      def nodesForPartitions(id: PartitionedId, partitions: java.util.Set[java.lang.Integer], capability: java.lang.Long) = nodesForPartitions(id, partitions, capability, 0L)
      def nodesForPartitions(id: PartitionedId, partitions:java.util.Set[java.lang.Integer], capability: java.lang.Long, persistentCapability: java.lang.Long) =  {
        val replica = scalaBalancer.nodesForPartitions(id, partitions, capability, persistentCapability)
        val result = new java.util.HashMap[JNode, java.util.Set[java.lang.Integer]](replica.size)

        replica.foreach { case (node, partitions) =>
          result.put(node, partitions)
                        }

        result
      }

      implicit def toOption(capability: java.lang.Long) : Option[Long] = {
        if (capability.longValue == 0L) None
        else Some(capability.longValue)
      }

      override def nextNodes(id: PartitionedId): util.LinkedHashSet[JNode] = nextNodes(id, 0L, 0L)

      override def nextNodes(id: PartitionedId, capability: lang.Long): util.LinkedHashSet[JNode] = nextNodes(id, capability, 0L)

      override def nextNodes(id: PartitionedId, capability: lang.Long, persistentCapability: lang.Long): util.LinkedHashSet[JNode] = {
        rewrap(scalaBalancer.nextNodes(id, Option(capability), Option(persistentCapability)))
      }

      def rewrap(nodes: util.LinkedHashSet[com.linkedin.norbert.cluster.Node]): util.LinkedHashSet[JNode] = {
        val result = new util.LinkedHashSet[JNode]()
        val it = nodes.iterator()
        while(it.hasNext) {
          val node:cluster.Node = it.next()
          result.add(new JavaNode(node.id, node.url, node.available, node.partitionIds, node.capability, node.persistentCapability))
        }
        result

      }

    }
    

  }

  def getNumPartitions(endpoints: java.util.Set[Endpoint]) = {
    scalaLbf.getNumPartitions(endpoints)
  }
}
