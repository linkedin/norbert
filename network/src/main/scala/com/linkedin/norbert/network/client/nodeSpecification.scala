/*
 * Partitioned and non-partitioned NodeSpecification wrapper objects for sendRequest
 */

package com.linkedin.norbert.network.client

trait NodeTrait[X] {
  var capability: Option[Long] = None;
  var persistentCapability: Option[Long] = None;
  def setCapability(cap: Option[Long]): X = {
    capability = cap;
    return this.asInstanceOf[X]
  }
  def setPersistentCapability(persistentCap: Option[Long]): X = {
    persistentCapability = persistentCap;
    return this.asInstanceOf[X]
  }
  def build: X = {
    if (capability == None && persistentCapability != None) {
      throw new IllegalArgumentException ("Cannot specify PersistentCapability without Capability")
    }
    return this.asInstanceOf[X]
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
  var numberOfReplicas: Option[Int] = None
  var clusterId: Option[Int] = None
  def bar(): Unit = {
    println("Bar.")
  }

  def setNumberOfReplicas(_numberOfReplicas: Option[Int]): PartitionedNodeSpec[_] = {
    this.numberOfReplicas = _numberOfReplicas
    this
  }

  def setClusterId(_clusterId: Option[Int]): PartitionedNodeSpec[_] = {
    this.clusterId = _clusterId
    this
  }
}


/*******************************************************
Testing
  ********************************************************/
object testing {
  def main(args: Array[String]): Unit = {
    try {

      val nonPartitionedTest = new NodeSpec()
        .setCapability(Some(1))
        .setPersistentCapability(Some(2))
        .build
      println("Non-Partitioned: " + "\n" + "Cap: " + nonPartitionedTest.capability + ", PersCap: " + nonPartitionedTest.persistentCapability)
      val PartitionedTest = new PartitionedNodeSpec(Set(1))
        .setCapability(Some(2))
        .setPersistentCapability(Some(3))
        .setNumberOfReplicas(Some(4))
        .setClusterId(Some(5))
        .build
      println("Partitioned: " + "\n" + "ids: " + PartitionedTest.ids + ", Cap: " + PartitionedTest.capability + ", PersCap: " + PartitionedTest.persistentCapability +
        ", NumRep: " + PartitionedTest.numberOfReplicas + ", cId: " + PartitionedTest.clusterId)
//      val nonPartitionedTestFail = new NodeSpec()
//        .setPersistentCapability(Some(2))
//        .build
//      println("Non-Partitioned Fail: " + "\n" + "Cap: " + nonPartitionedTestFail.capability + ", PersCap: " + nonPartitionedTestFail.persistentCapability)
      val PartitionedTestFail = new PartitionedNodeSpec(Set(1))
        .setPersistentCapability(Some(2))
        .build
      println("Non-Partitioned Fail: " + "\n" + "Cap: " + PartitionedTestFail.capability + ", PersCap: " + PartitionedTestFail.persistentCapability)
    }
    catch {
      case e: Exception => println("There was an exception: " + e)
    }
  }
}


