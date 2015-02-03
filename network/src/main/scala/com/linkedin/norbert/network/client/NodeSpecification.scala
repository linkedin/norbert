/*
 * Partitioned and non-partitioned NodeSpecification wrapper objects for sendRequest
 */
package com.linkedin.norbert
package network
package client


trait NodeTrait[NodeType] {
  var capability: Option[Long] = None
  var persistentCapability: Option[Long] = None

  def setCapability(cap: Option[Long]): this.type = {
    capability = cap
    this
  }

  def setPersistentCapability(persistentCap: Option[Long]): this.type = {
    persistentCapability = persistentCap
    this
  }

  def build: this.type = {
    capability match {
      case Some(cap) => this
      case None =>
        persistentCapability match {
          case Some(persCap) => throw new IllegalArgumentException("Cannot specify PersistentCapability without Capability")
          case None => this
        }
    }
  }
}

///*********************************
//Non-Partitioned NodeSpecification
//*********************************/
class NodeSpec extends NodeTrait[NodeSpec]

///*******************************************************
//Partitioned NodeSpecification
//********************************************************/

class PartitionedNodeSpec[PartitionedId](val ids: Set[PartitionedId]) extends NodeTrait[PartitionedNodeSpec[_]] {
  var numberOfReplicas: Int = 0
  var clusterId: Option[Int] = None

  def setNumberOfReplicas(_numberOfReplicas: Int): PartitionedNodeSpec[_] = {
    this.numberOfReplicas = _numberOfReplicas
    this
  }

  def setClusterId(_clusterId: Option[Int]): PartitionedNodeSpec[_] = {
    this.clusterId = _clusterId
    this
  }
}




