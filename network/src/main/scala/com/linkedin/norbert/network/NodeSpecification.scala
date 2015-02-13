/**
 * Partitioned and non-partitioned NodeSpecification wrapper objects for sendRequest
 */
package com.linkedin.norbert.network

import com.linkedin.norbert.network.javaobjects.{NodeSpecification => JNodeSpecification, PartitionedNodeSpecification => JPartitionedNodeSpecification}

/**
 * A NodeSpecification object is used to store the necessary information to specify a node.
 * For the non-partitioned version this is the capability and persistentCapability.
 * The Partitioned version extends the non-partitioned version with numberOfReplicas and clusterId
 */

/**
 * NodeTrait is the trait that the NodeSpecification objects extend.

 * @tparam NodeType Either a non-partitioned or partitioned nodeSpec
 */
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

/**
* Non-Partitioned NodeSpecification
*/
class NodeSpecification extends NodeTrait[NodeSpecification] with JNodeSpecification {
  def getCapability() = capability;
  def getPersistentCapability = persistentCapability;
}

/**
* Partitioned NodeSpecification
*/

class PartitionedNodeSpecification[PartitionedId](val ids: Set[PartitionedId]) extends NodeTrait[PartitionedNodeSpecification[_]] with JPartitionedNodeSpecification[PartitionedId] {
  var numberOfReplicas: Int = 0
  var clusterId: Option[Int] = None

  def setNumberOfReplicas(_numberOfReplicas: Int): PartitionedNodeSpecification[PartitionedId] = {
    this.numberOfReplicas = _numberOfReplicas
    this
  }

  def setClusterId(_clusterId: Option[Int]): PartitionedNodeSpecification[PartitionedId] = {
    this.clusterId = _clusterId
    this
  }

  def getNumberofReplicas() = numberOfReplicas;
  def getClusterId() = clusterId;
}




