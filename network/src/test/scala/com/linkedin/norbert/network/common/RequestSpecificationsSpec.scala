/*
 * Partitioned and non-partitioned RequestSpecification tests using specs
 */

package com.linkedin.norbert
package network
package server

import org.specs.SpecificationWithJUnit
import org.specs.mock.Mockito
import network.common.SampleMessage
import scala.collection.mutable.MutableList

class RequestSpecificationsSpec extends SpecificationWithJUnit {

  val requestSpecificationTest: RequestSpecification[String] = RequestSpecification[String]("test")
  val partitionedRequestSpecificationTest2: PartitionedRequestSpecification[String, Int] = PartitionedRequestSpecification[String, Int](Some("partitionedTest"))

  "RequestSpecification" should {
    "create a new RequestSpecification object" in {
      requestSpecificationTest.message must be equalTo("test")
    }

    "convert a RequestSpecification object to a PartitionedRequestSpecification object" in {
      val partitionedRequestSpecificationTest: PartitionedRequestSpecification[String, Int] = requestSpecificationTest
      partitionedRequestSpecificationTest.message must beSome("test")
    }

    "create a new PartitionedRequestSpecification object" in {
      partitionedRequestSpecificationTest2.message must beSome("partitionedTest")
    }

    "convert a PartitionedRequestSpecification object to a RequestSpecification object" in {
      val requestSpecificationTest2: RequestSpecification[String] = partitionedRequestSpecificationTest2
      requestSpecificationTest2.message must be equalTo("partitionedTest")
    }


  }
}

