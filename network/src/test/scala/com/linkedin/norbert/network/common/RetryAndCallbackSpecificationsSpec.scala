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

class RetryAndCallbackSpecificationsSpec extends SpecificationWithJUnit {


  "RetryAndCallbackSpecification" should {
    "create a new retrySpecifications object" in {
      val retrySpecificationsTest: RetrySpecifications[String] = RetrySpecifications[String](9)
      retrySpecificationsTest.maxRetry must be equalTo(9)
    }

    "create a new partitionedRetrySpecifications object" in {
      val partitionedRetrySpecificationsTest: PartitionedRetrySpecifications[String] = PartitionedRetrySpecifications[String](9)
      partitionedRetrySpecificationsTest.maxRetry must be equalTo(9)
    }


  }
}

