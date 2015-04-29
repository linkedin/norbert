/*
 * Partitioned and non-partitioned retrySpecification tests using specs
 */

package com.linkedin.norbert
package network
package server

import com.linkedin.norbert.network.partitioned.{PartitionedRetrySpecification, RetrySpecification}
import org.specs.SpecificationWithJUnit

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
