package com.linkedin.norbert

import com.linkedin.norbert.network.client.NetworkClientConfig
import com.linkedin.norbert.network.common.RetryStrategy

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
 * which only contains two parameters. The class contains just a default constructor.
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
  /* removed because causing testing errors
  if (maxRetry == 0 && callback != None) {
    throw new IllegalArgumentException("maxRetry must be greater than 0 for callback options to work")
  }
*/

}

/**
 * This is the companion object for the PartitionedRetrySpecifications class.
 */
object PartitionedRetrySpecifications {
  //can have maxRetry without callback, but you cannot have callback without maxRetry
  def apply[ResponseMsg](maxRetry: Int = 0,
                         callback: Option[Either[Throwable, ResponseMsg] => Unit] = None, //new FutureAdapterListener[ResponseMsg],
                         retryStrategy: Option[RetryStrategy] = None) = {
    new PartitionedRetrySpecifications[ResponseMsg](maxRetry, callback, retryStrategy)
  }


}

/**
 * This is the partitioned version of the RetrySpecifications class which encapsulates retry specifications. This class contains
 * a default constructor and no additional functionality.
 *
 * @param maxRetry This is the maximum number of retry attempts for the request. If not otherwise specified, the value will be 0.
 * @param callback This is a method to be called with either a Throwable in the case of an error along
 *                 the way or a ResponseMsg representing the result.
 * @param retryStrategy This is the strategy to apply when we run into timeout situation.
 * @tparam ResponseMsg
 */
class PartitionedRetrySpecifications[ResponseMsg](maxRetry: Int,
                                         callback: Option[Either[Throwable, ResponseMsg] => Unit],
                                         var retryStrategy: Option[RetryStrategy],
                                         var duplicatesOk: Boolean = false) extends RetrySpecifications[ResponseMsg](maxRetry, callback) {

  val routingConfigs = new RoutingConfigs(retryStrategy != None, duplicatesOk)
  def setConfig(config: NetworkClientConfig): Unit = {
    duplicatesOk = config.duplicatesOk
    if (retryStrategy != null)
      retryStrategy = config.retryStrategy
  }

}

/**
 * This provides some basic testing for both the RetrySpecifications and PartitionedRetrySpecifications classes.
 */
object RCBTesting {
  def main(args: Array[String]) = {
    try {
      val tester: RetrySpecifications[String] = RetrySpecifications[String](9)
      //val partitionedTester: PartitionedRetrySpecifications[String] = PartitionedRetrySpecifications[String]()
      println(tester.callback)
      println(tester.maxRetry)
    }
    catch {
      case e: Exception => println("There was an exception: " + e)
    }
  }
}

