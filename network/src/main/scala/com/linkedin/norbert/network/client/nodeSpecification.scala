/*
 * Partitioned and non-partitioned NodeSpecification wrapper objects for sendRequest
 */

package com.linkedin.norbert.network.client

abstract class Product

trait PartitionedNodeTrait {
  var numberOfReplicas:  Option[Int] = None
  var clusterId:  Option[Int] = None
  def withNumberOfReplicas(numberOfReplicas: Option[Int] = None): PartitionedNodeTrait = {
    this.numberOfReplicas = numberOfReplicas
    this
  }

  def withClusterId(clusterId: Option[Int] = None): PartitionedNodeTrait = {
    this.clusterId = clusterId
    this
  }
}

/*********************************
Non-Partitioned NodeSpecification
*********************************/

// Abstract builder class
class NodeSpecificationBuilder{
  var capability: Option[Long] = None
  var persistentCapability: Option[Long] = None

  def withCapability(capability: Option[Long]): NodeSpecificationBuilder = {
    this.capability = capability
    this
  }
  def withPersistentCapability(persistentCapability: Option[Long]): NodeSpecificationBuilder = {
    this.persistentCapability = persistentCapability
    this
  }

  def build: Product = new NodeSpec(this)
}

// test invalid combos here
class NodeSpec(builder: NodeSpecificationBuilder) extends Product {
  val capability: Option[Long] = builder.capability
  val persistentCapability: Option[Long] = builder.persistentCapability


  if (capability == None && persistentCapability != None) {
    throw new IllegalArgumentException("Cannot specify PersistentCapability without Capability")
  }


  override def toString: String = {
    " Capability:" + capability + " PersistentCapability:" + persistentCapability
  }
}



/*******************************************************
Partitioned NodeSpecification
********************************************************/


// test invalid combos here
class PartitionedNodeSpec(builder: PartitionedNodeSpecificationBuilder[_]) extends Product {
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
class PartitionedNodeSpecificationBuilder[PartitionedId](val ids: Set[PartitionedId]) extends NodeSpecificationBuilder with PartitionedNodeTrait {
/*  var numberOfReplicas:  Option[Int] = None
  var clusterId:  Option[Int] = None*/

  if (ids == None) {
    throw new IllegalArgumentException("PartitionedId must be specified")
  }
  else {
    println(ids)
  }


  override def withCapability(capability: Option[Long] = None): PartitionedNodeSpecificationBuilder[_] = {
    this.capability = capability
    this
  }

  override def withPersistentCapability(persistentCapability: Option[Long] = None): PartitionedNodeSpecificationBuilder[_] = {
    this.persistentCapability = persistentCapability
    this
  }

  override def withNumberOfReplicas(numberOfReplicas: Option[Int] = None): PartitionedNodeSpecificationBuilder[_] = {
    this.numberOfReplicas = numberOfReplicas
    this
  }

  override def withClusterId(clusterId: Option[Int] = None): PartitionedNodeSpecificationBuilder[_] = {
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
      val nonPartitionedTest = new NodeSpecificationBuilder().withCapability(Some(1)).build
      println("nonPartitioned:" + nonPartitionedTest)
     // val nonPartitionedTestGet = nonPartitionedTest
      val PartitionedTest = new PartitionedNodeSpecificationBuilder(Set{5}).withCapability(Some(1)).withClusterId(Some(5)).build
      println("Partitioned:" + PartitionedTest)
      val failingNonPartitionedTest = new NodeSpecificationBuilder().withPersistentCapability(Some(1)).build
      println("nonPartitioned:" + failingNonPartitionedTest)
      val failingPartitionedTest = new PartitionedNodeSpecificationBuilder(Set{5}).withPersistentCapability(Some(1)).withClusterId(Some(5)).build
      println("failingPartitioned:" + PartitionedTest)
    }
    catch {
      case e: Exception => println("There was an exception: " + e)
    }
  }
}


