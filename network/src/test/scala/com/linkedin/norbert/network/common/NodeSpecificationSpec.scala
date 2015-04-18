/*
 * Copyright 2009-2010 LinkedIn, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.linkedin.norbert
package network
package client

import org.specs.SpecificationWithJUnit
import org.specs.mock.Mockito
import network.common.SampleMessage
import scala.collection.mutable.MutableList
import com.linkedin.norbert.network.client

/*
 * Partitioned and non-partitioned nodeSpecification tests using specs
 */
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

    "create a new NodeSpecification object if altPort is set" in {
      val altPortNonPartitionedTest = new NodeSpecification()
        .setCapability(Some(1))
        .setPersistentCapability(Some(2))
        .setAltPort(Some(3))
        .build
      altPortNonPartitionedTest.capability must beSome(1)
      altPortNonPartitionedTest.persistentCapability must beSome(2)
      altPortNonPartitionedTest.altPort must beSome(3)
    }

    "create a new PartitionedNodeSpecification object if altPort is set" in {
      val altPortPartitionedTest = new PartitionedNodeSpecification(Set(1))
        .setCapability(Some(2))
        .setPersistentCapability(Some(3))
        .setNumberOfReplicas(4)
        .setClusterId(Some(5))
        .setAltPort(Some(6))
        .build
      altPortPartitionedTest.capability must beSome(2)
      altPortPartitionedTest.persistentCapability must beSome(3)
      altPortPartitionedTest.numberOfReplicas must be equalTo(4)
      altPortPartitionedTest.clusterId must beSome(5)
      altPortPartitionedTest.altPort must beSome(6)
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

