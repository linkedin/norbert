package com.linkedin.norbert

import _root_.com.linkedin.norbert.network.client.NetworkClientConfig
import _root_.com.linkedin.norbert.network.common.{FutureAdapterListener, RetryStrategy}
import com.linkedin.norbert.cluster.ClusterDisconnectedException
import com.linkedin.norbert.network._
import com.linkedin.norbert.network.client.NetworkClientConfig
import com.linkedin.norbert.network.common.RetryStrategy
import com.linkedin.norbert.network.partitioned.RoutingConfigs

/**
 * This is the companion object for the RoutingConfigs class.
 */
object RoutingConfigs {
  val defaultRoutingConfigs = new RoutingConfigs(false, false)
  def getDefaultRoutingConfigs():RoutingConfigs = {
    defaultRoutingConfigs
  }
}

/**
 * This class encapsulates the parameters used for request routing configurations.
 * @param SelectiveRetry This indicates whether or not we want to use a specific retry strategy.
 * @param DuplicatesOk This indicates whether or not we can have duplicates returned to a higher application layer.
 */
class RoutingConfigs(SelectiveRetry: Boolean, DuplicatesOk: Boolean ) {
  val selectiveRetry = SelectiveRetry
  val duplicatesOk = DuplicatesOk
}

/**
 * This is the companion object for the RetrySpecifications class.
 */
object RetrySpecifications {
  def apply[ResponseMsg](maxRetry: Int = 0,
                         callback: Option[Either[Throwable, ResponseMsg] => Unit] = None) = {
    new RetrySpecifications[ResponseMsg](maxRetry, callback)
  }

}

/**
 * This class encapsulates the retry specifications for a request. This class is the non-partitioned version
 * which only contains two parameters. The class contains a default constructor and a check for valid inputs.
 *
 * @param maxRetry This is the maximum number of retry attempts for the request. If not otherwise specified, the value will be 0.
 * @param callback This is a method to be called with either a Throwable in the case of an error along
 *                 the way or a ResponseMsg representing the result.
 *
 * @throws IllegalArgumentException if the value for maxRetry is less than 0 and the callback is specified.
 */
class RetrySpecifications[ResponseMsg](val maxRetry: Int,
                                                  val callback: Option[Either[Throwable, ResponseMsg] => Unit]) {
  //Validation checks go here:
  if (maxRetry == 0 && callback != None) {
    throw new IllegalArgumentException("maxRetry must be greater than 0 for callback options to work")
  }
}

/**
 * This is the companion object for the PartitionedRetrySpecifications class.
 */
object PartitionedRetrySpecifications {
  //can have maxRetry without callback, but you cannot have callback without maxRetry
  def apply[ResponseMsg](maxRetry: Int = 0,
                         callback: Option[Either[Throwable, ResponseMsg] => Unit] = None, //new FutureAdapterListener[ResponseMsg],
                         routingConfigs: RoutingConfigs = new RoutingConfigs(retryStrategy != None, duplicatesOk),
                         retryStrategy: Option[RetryStrategy] = retryStrategy) = {
    new PartitionedRetrySpecifications[ResponseMsg](maxRetry, callback, routingConfigs, retryStrategy)
  }

  var duplicatesOk: Boolean = false
  var retryStrategy: Option[RetryStrategy] = None

  def setConfig(config: NetworkClientConfig): Unit = {
    duplicatesOk = config.duplicatesOk
    if (retryStrategy != null)
      retryStrategy = config.retryStrategy
  }
}

/**
 * This is the partitioned version of the RetrySpecifications class which encapsulates retry specifications. This class contains
 * a default constructor and no additional functionality.
 *
 * @param maxRetry This is the maximum number of retry attempts for the request. If not otherwise specified, the value will be 0.
 * @param callback This is a method to be called with either a Throwable in the case of an error along
 *                 the way or a ResponseMsg representing the result.
 * @param routingConfigs This contains information regarding options for a request routing strategy.
 * @param retryStrategy This is the strategy to apply when we run into timeout situation.
 * @tparam ResponseMsg
 */
class PartitionedRetrySpecifications[ResponseMsg](maxRetry: Int,
                                         callback: Option[Either[Throwable, ResponseMsg] => Unit],
                                         val routingConfigs: RoutingConfigs,
                                         val retryStrategy: Option[RetryStrategy]) extends RetrySpecifications[ResponseMsg](maxRetry, callback) {
//  Validation checks go here (possibly not needed since they are in the class that this one extends)
//  if (maxRetry == 0 && callback != None) {
//    throw new IllegalArgumentException("maxRetry must be greater than 0 for callback options to work")
//  }
}

/**
 * This provides some basic testing for both the RetrySpecifications and PartitionedRetrySpecifications classes.
 */
object testing {
  def main(args: Array[String]) = {
    try {
      val tester: RetrySpecifications[String] = RetrySpecifications[String](9)
      //val partitionedTester: PartitionedRetrySpecifications[String] = PartitionedRetrySpecifications[String]()
      println(tester.callback)
      println(tester.maxRetry)
      //println(partitionedTester.routingConfigs)
    }
    catch {
      case e: Exception => println("There was an exception: " + e)
    }
  }
}

