//// Specifications for the number of retry attempts and any callback function to be used.
//package com.linkedin.norbert.network
//
//import com.linkedin.norbert.network.partitioned.RoutingConfigs
//
//
//object RetryCallbackSpecs {
//  def apply[ResponseMsg](maxRetry: Int, callback: Int) = {
//    new RetryCallbackSpecs(maxRetry, callback)
//  }
//}
//
//class RetryCallbackSpecs(val maxRetry: Int = 17, val callback: Int = 11) { //val callback: Either[Throwable, ResponseMsg] => Unit) {
//
//  // Things we'll want:
//  //    maxRetry
//  //    callback
//  def this(maxRetry) = this(maxRetry, 7)
//  def yayAFunction(): Int = {
//    return maxRetry + callback
//  }
//
//
//
//  // TODO: Also ResponseAggregator? This is something we must ask about at our next meeting.
//}
//
//trait RetryCallbackSpecsAccess[RetryCallbackSpecs] {
//  def retryCallbackSpecs: RetryCallbackSpecs
//}
//
//object TestRetryCallbackSpecs{
//  def main(args: Array[String]): Unit = {
////    val testRoutingConfigs = new RoutingConfigs(false, false);
////    println(testRoutingConfigs);
//    val testObject = new RetryCallbackSpecs();
//    println("Something happens!");
//    println(testObject.yayAFunction());
//  }
//}
//
////TestRetryCallbackSpecs.main(0,0);