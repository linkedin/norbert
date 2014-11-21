/*
 * Partitioned and non-partitioned NodeSpecification wrapper objects for sendRequest
 */

package com.linkedin.norbert.network.client

abstract class Product

/*********************************
Non-Partitioned NodeSpecification
*********************************/

// Abstract builder class
abstract class NodeSpecificationBuilder {
  var capability: Option[Long]
  var persistentCapability: Option[Long]

  def withCapability(capability: Option[Long]): NodeSpecificationBuilder
  def withPersistentCapability(persistentCapability: Option[Long]): NodeSpecificationBuilder

  def build: Product
}

// test invalid combos here
class nodeSpec(builder: NodeSpecificationBuilder) extends Product {
  val capability: Option[Long] = builder.capability
  val persistentCapability: Option[Long] = builder.persistentCapability


  if (capability == None && persistentCapability != None) {
    throw new IllegalArgumentException("Cannot specify PersistentCapability without Capability")
  }


  override def toString: String = {
    " Capability:" + capability + " PersistentCapability:" + persistentCapability
  }
}

// Builder subclass
class NodeSpecification extends NodeSpecificationBuilder {
  var capability: Option[Long] = None
  var persistentCapability: Option[Long] = None

  override def withCapability(capability: Option[Long] = None): NodeSpecificationBuilder = {
    this.capability = capability
    this
  }

  override def withPersistentCapability(persistentCapability: Option[Long] = None): NodeSpecificationBuilder = {
    this.persistentCapability = persistentCapability
    this
  }

  override def build: Product = new nodeSpec(this)
}


/*******************************************************
Partitioned NodeSpecification
********************************************************/

// Abstract Builder class
abstract class PartitionedNodeSpecificationBuilder extends NodeSpecificationBuilder{

  var numberOfReplicas:  Option[Int]
  var clusterId:  Option[Int]

  def withNumberOfReplicas(numberOfReplicas: Option[Int]): PartitionedNodeSpecificationBuilder
  def withClusterId(clusterId: Option[Int]): PartitionedNodeSpecificationBuilder

  def build: Product
}


// test invalid combos here
class PartitionedNodeSpec(builder: PartitionedNodeSpecificationBuilder) extends Product {
  val capability: Option[Long] = builder.capability
  val persistentCapability: Option[Long] = builder.persistentCapability
  val numberOfReplicas:  Option[Int] = builder.numberOfReplicas
  val clusterId:  Option[Int] = builder.clusterId


  if (capability == None && persistentCapability != None) {
    throw new IllegalArgumentException("Cannot specify PersistentCapability without Capability")
  }

  override def toString: String = {
    " Capability:" + capability + " PersistentCapability:" + persistentCapability + " Replicas:" + numberOfReplicas + " clusterId:" + clusterId
  }
}

// builder subclass
class PartitionedNodeSpecification[PartitionedId](val ids: Set[PartitionedId]) extends PartitionedNodeSpecificationBuilder {
  var capability: Option[Long] = None
  var persistentCapability: Option[Long] = None
  var numberOfReplicas:  Option[Int] = None
  var clusterId:  Option[Int] = None

  if (ids == None) {
    throw new IllegalArgumentException("PartitionedId must be specified")
  }
  else {
    println(ids)
  }


  override def withCapability(capability: Option[Long] = None): PartitionedNodeSpecificationBuilder = {
    this.capability = capability
    this
  }

  override def withPersistentCapability(persistentCapability: Option[Long] = None): PartitionedNodeSpecificationBuilder = {
    this.persistentCapability = persistentCapability
    this
  }

  override def withNumberOfReplicas(numberOfReplicas: Option[Int] = None): PartitionedNodeSpecificationBuilder = {
    this.numberOfReplicas = numberOfReplicas
    this
  }

  override def withClusterId(clusterId: Option[Int] = None): PartitionedNodeSpecificationBuilder = {
    this.clusterId = clusterId
    this
  }

  override def build: Product = new PartitionedNodeSpec(this)
}

/*******************************************************
Testing
  ********************************************************/
object testing {
  def main(args: Array[String]): Unit = {
    try {
      val nonPartitionedTest = new NodeSpecification().withCapability(Some(1)).build
      println("nonPartitioned:" + nonPartitionedTest)
      val PartitionedTest = new PartitionedNodeSpecification(Set{5}).withCapability(Some(1)).withClusterId(Some(5)).build
      println("Partitioned:" + PartitionedTest)
      val failingNonPartitionedTest = new NodeSpecification().withPersistentCapability(Some(1)).build
      println("nonPartitioned:" + failingNonPartitionedTest)
      val failingPartitionedTest = new PartitionedNodeSpecification(Set{5}).withPersistentCapability(Some(1)).withClusterId(Some(5)).build
      println("failingPartitioned:" + PartitionedTest)
    }
    catch {
      case e: Exception => println("There was an exception: " + e)
    }
  }
}


