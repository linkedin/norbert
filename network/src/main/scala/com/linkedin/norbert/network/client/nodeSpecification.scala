/*
 * Partitioned and non-partitioned NodeSpecification wrapper objects for sendRequest
 */

package com.linkedin.norbert.network.client

/*
 * Non-partitioned NodeSpecifications
 */

object NodeSpecifications {
  def apply(capability: Option[Long] = None, persistentCapability:  Option[Long] = None): NodeSpecifications = {
    new NodeSpecifications(capability, persistentCapability)
  }
}


class NodeSpecifications(val Capability: Option[Long], val PersistentCapability:  Option[Long]) {
  if (Capability == None && PersistentCapability != None) {
    throw new IllegalArgumentException("Cannot specify PersistentCapability without Capability")
  }
}


/*
 * Partitioned NodeSpecifications
 */

object PartitionedNodeSpecifications{
  def apply[PartitionedId](ids: Set[PartitionedId], capability: Option[Long] = None, persistentCapability:  Option[Long] = None, numberOfReplicas:  Int = 0, clusterId:  Option[Int] = None): NodeSpecifications = {
    new PartitionedNodeSpecifications(ids, capability, persistentCapability, numberOfReplicas, clusterId)

  }
}


class PartitionedNodeSpecifications[PartitionedId](val ids: Set[PartitionedId], override val Capability: Option[Long], override val PersistentCapability:  Option[Long],
                         val NumberOfReplicas:  Int, val ClusterId:  Option[Int]) extends NodeSpecifications(Capability, PersistentCapability) {
  if (Capability == None && PersistentCapability != None) {
    throw new IllegalArgumentException("Cannot specify PersistentCapability without Capability")
  }
  if (ids == None) {
    throw new IllegalArgumentException("PartitionedId must be specified")
  }
}



/*
 * Tests
 */

object testing {
  def main(args: Array[String]): Unit = {
    try {
      val ids = Set{1}
      val capability, persistentCapability, numberOfReplicas, clusterId = 1
      var nonPartitionedTest = NodeSpecifications(Some(capability));
      var partitionedTest = PartitionedNodeSpecifications[Int](Set{5});
    }
    catch {
      case e: Exception => println("There was an exception: " + e)
    }
  }
}