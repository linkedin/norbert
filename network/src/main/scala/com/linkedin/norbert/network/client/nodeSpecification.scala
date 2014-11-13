package com.linkedin.norbert.network.client


object nodeSpecifications {
  def apply(capability: Option[Long] = None, persistentCapability:  Option[Long] = None): nodeSpecifications = {
    new nodeSpecifications(capability, persistentCapability)
  }
}


class nodeSpecifications(val Capability: Option[Long], val PersistentCapability:  Option[Long]) {
  if (Capability == None && PersistentCapability != None) {
    throw new IllegalArgumentException("need to specify persistentCapability")
  }
}



object partitionedNodeSpecifications{
  def apply[PartitionedId](ids: Option[Set[PartitionedId]] = None, capability: Option[Long] = None, persistentCapability:  Option[Long] = None, numberOfReplicas:  Int = 0, clusterId:  Option[Int] = None): nodeSpecifications = {
    new partitionedNodeSpecifications(ids, capability, persistentCapability, numberOfReplicas, clusterId)

  }
}


class partitionedNodeSpecifications[PartitionedId](val ids: Option[Set[PartitionedId]], override val Capability: Option[Long], override val PersistentCapability:  Option[Long],
                         val NumberOfReplicas:  Int, val ClusterId:  Option[Int]) extends nodeSpecifications(Capability, PersistentCapability) {
  if (Capability == None && PersistentCapability != None) {
    throw new IllegalArgumentException("need to specify capability")
  }
  if (ids == None) {
    throw new IllegalArgumentException("need to specify PartitionedId")
  }
}



object testing {
  def main(args: Array[String]): Unit = {
    try {
      var test = nodeSpecifications(Some(5));
      var partitionedTest = partitionedNodeSpecifications[Int]();
    }
    catch {
      case e: Exception => println("There was an exception: " + e)
    }
  }
}