/*
 * Partitioned and non-partitioned nodeSpecification tests using specs
 */

package com.linkedin.norbert
package network
package client

import org.specs.SpecificationWithJUnit
import org.specs.mock.Mockito
import network.common.SampleMessage
import scala.collection.mutable.MutableList
import com.linkedin.norbert.network.client

class NodeSpecificationSpec extends SpecificationWithJUnit {

  "NodeSpecification" should {
    "create a new NodeSpecification object if capability is set" in {
      val nonPartitionedTest = new NodeSpecification()
        .setCapability(Some(1))
        .setPersistentCapability(Some(2))
        .build
      nonPartitionedTest.capability must beSome(1)
      nonPartitionedTest.persistentCapability must beSome(2)

    }

    "create a new PartitionedNodeSpecification object if capability is set" in {
      val PartitionedTest = new PartitionedNodeSpecification(Set(1))
        .setCapability(Some(2))
        .setPersistentCapability(Some(3))
        .setNumberOfReplicas(4)
        .setClusterId(Some(5))
        .build
      PartitionedTest.capability must beSome(2)
      PartitionedTest.persistentCapability must beSome(3)
      PartitionedTest.numberOfReplicas must be equalTo(4)
      PartitionedTest.clusterId must beSome(5)
    }

    "throw an IllegalArgumentException if persistentCapability is set but not capability" in {

      new NodeSpecification()
        .setPersistentCapability(Some(2))
        .build must throwA[IllegalArgumentException]
      new PartitionedNodeSpecification(Set(1))
        .setPersistentCapability(Some(2))
        .build must throwA[IllegalArgumentException]
    }

  }
}

