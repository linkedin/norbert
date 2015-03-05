/*
 * Partitioned and non-partitioned retrySpecification tests using specs
 */

package com.linkedin.norbert
package network
package server

import org.specs.SpecificationWithJUnit
import org.specs.mock.Mockito
import network.common.SampleMessage
import scala.collection.mutable.MutableList
import com.linkedin.norbert.network

class RetryAndCallbackSpecificationSpec extends SpecificationWithJUnit {


  "RetryAndCallbackSpecification" should {
    "create a new retrySpecification object" in {
      val retrySpecificationTest: RetrySpecification[String] = RetrySpecification[String](9)
      retrySpecificationTest.getMaxRetry() must be equalTo(9)
    }

    "create a new partitionedRetrySpecification object" in {
      val partitionedRetrySpecificationTest: PartitionedRetrySpecification[String] = PartitionedRetrySpecification[String](9)
      partitionedRetrySpecificationTest.getMaxRetry() must be equalTo(9)
    }


  }
}

